"""Bot's main file."""
import logging
from os import getenv

from dotenv import load_dotenv
from telegram import (InlineKeyboardButton, InlineKeyboardMarkup,
                      InputMediaPhoto, Update)
from telegram.ext import (CallbackContext, CallbackQueryHandler,
                          CommandHandler, Filters, MessageHandler, Updater)

from . import constants
from . import user_orm
from .decorators import safe_handler_method
from .exceptions import BadRequestError, LangNotChosenError, NoTokenError
from display_data import buttons, texts
from utils.paginators import Paginator
from utils.shortcuts import send_photo

logger = logging.getLogger('__main__')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s '
                              '(def %(funcName)s:%(lineno)d)')
handler = logging.handlers.RotatingFileHandler(
    'logs/bot.log', maxBytes=5*1024*1024, backupCount=2
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def check_token(token) -> None:
    """Проверка, существует ли токен."""
    if token is None:
        logger.critical('Отсутствует необходимая переменная окружения (token)')
        raise NoTokenError('No Token')


def handle_user_lang(update: Update, context: CallbackContext, callback_fn):
    """Получает и проверяет язык пользователя, выполняет callback функцию."""
    chat_id = update.effective_chat.id
    user = user_orm.User(chat_id)
    lang = user.get_field('lang')
    if lang is None:
        raise LangNotChosenError
    callback_fn(lang, update, context)


def create_inline_keyboard(buttons_data, lang=None):
    """Создание Inline-клавиатуры с данными кнопок."""
    keyboard = [[InlineKeyboardButton(button[lang] if lang else button,
                                      callback_data=data)]
                for button, data in buttons_data]
    return InlineKeyboardMarkup(keyboard)


@safe_handler_method
def start(update: Update, context: CallbackContext) -> None:
    """Отправить выбор языка."""
    user_orm.create_user(update.effective_chat.id)
    keyboard_data = [
        (buttons.TJ_LANG_CHOOSE_BUTTON,
         f'{constants.LANG_PATTERN}{constants.TJ}'),
        (buttons.RU_LANG_CHOOSE_BUTTON,
         f'{constants.LANG_PATTERN}{constants.RU}')
    ]
    reply_markup = create_inline_keyboard(keyboard_data)
    message = update.message.reply_text(
        texts.CHOOSE_LANG_TEXT, reply_markup=reply_markup)
    context.bot_data['lang_message_id'] = message.message_id
    update.message.delete()


@safe_handler_method
def save_lang(update: Update, context: CallbackContext) -> None:
    """Сохранить язык пользователя и вызвать главное меню."""
    query = update.callback_query
    lang = query.data.split(constants.LANG_PATTERN)[1]
    chat_id = update.effective_chat.id

    if lang not in constants.LANGUAGES:
        logger.error('Некорректный формат языка')
        raise BadRequestError('Lang is in incorrect format.')

    user_orm.User(chat_id).edit_field('lang', lang)
    main_menu(update, context)


@safe_handler_method
def main_menu(update: Update, context: CallbackContext) -> None:
    """Отправить главное меню."""
    def show_menu(lang, update, context):
        query = update.callback_query
        chat_id = update.effective_chat.id
        keyboard_data = [
            (buttons.COURSE_BUTTON, constants.COURSES_CALLBACK),
            (buttons.ABOUT_ACADEMY_BUTTON, constants.ACADEMY_DESC_CALLBACK),
            (buttons.CONTACT_INFO_BUTTON, constants.CONTACT_INFO_CALLBACK)
        ]
        reply_markup = create_inline_keyboard(keyboard_data, lang)

        user_object = user_orm.User(chat_id)
        if not user_object.get_field('is_main_message'):
            send_photo(url='logo.jpg', bot=context.bot, chat_id=chat_id,
                       caption=texts.WELCOME_TEXT[lang],
                       reply_markup=reply_markup, parse_mode='HTML')
            user_object.edit_field('is_main_message', 1)
        else:
            query.edit_message_caption(texts.WELCOME_TEXT[lang], reply_markup)

        if 'lang_message_id' in context.bot_data:
            context.bot.delete_message(chat_id,
                                       context.bot_data['lang_message_id'])
            context.bot_data.pop('lang_message_id')
    handle_user_lang(update, context, show_menu)


@safe_handler_method
def contact_info(update: Update, context: CallbackContext) -> None:
    """Отправить контактную информацию."""
    def show_contact_info(lang, update, context):
        query = update.callback_query
        keyboard_data = [
            (buttons.INSTAGRAM_BUTTON, constants.INSTAGRAM_URL),
            (buttons.TELEGRAM_CHANNEL_BUTTON, constants.TELEGRAM_CHANNEL_URL),
            (buttons.BACK_BUTTON, constants.MAIN_MENU_CALLBACK)
        ]
        reply_markup = create_inline_keyboard(keyboard_data, lang)
        query.edit_message_caption(
            texts.CONTACTS_TEXT[lang], reply_markup, parse_mode='HTML')

    handle_user_lang(update, context, show_contact_info)


@safe_handler_method
def about_academy(update: Update, context: CallbackContext) -> None:
    """Отправить информацию об академии."""
    def show_about_academy(lang, update, context):
        query = update.callback_query
        keyboard_data = [(buttons.BACK_BUTTON, constants.MAIN_MENU_CALLBACK)]
        reply_markup = create_inline_keyboard(keyboard_data, lang)
        query.edit_message_caption(
            texts.ABOUT_ACADEMY_TEXT[lang], reply_markup, parse_mode='HTML')

    handle_user_lang(update, context, show_about_academy)


@safe_handler_method
def courses(update: Update, context: CallbackContext) -> None:
    """Отправить список курсов."""
    def show_courses(lang, update, context):
        query = update.callback_query
        page = int(query.data.split(constants.COURSES_PATTERN)[1])
        paginator = Paginator(texts.COURSES, constants.COURSES_PATTERN,
                              constants.ITEMS_PER_PAGE)
        keyboard = [[InlineKeyboardButton(
            course[lang]['button_text'][:16],
            callback_data=f'{constants.COURSE_PATTERN}{callback_data}')]
                    for callback_data, course in paginator.get_page(page)]

        keyboard.append(paginator.create_pagination_buttons(page))
        keyboard.append([InlineKeyboardButton(
            buttons.BACK_BUTTON[lang],
            callback_data=constants.MAIN_MENU_CALLBACK)])

        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_caption(texts.COURSES_LIST_TEXT[lang], reply_markup)

    handle_user_lang(update, context, show_courses)


@safe_handler_method
def course_info(update: Update, context: CallbackContext) -> None:
    """Отправить информацию о курсе."""
    def show_course_info(lang, update, context):
        query = update.callback_query
        chat_id = update.effective_chat.id
        course_name = query.data.split('_')[1]
        keyboard_data = [
            (buttons.REGISTER_BUTTON, constants.REGISTER_URL),
            (buttons.BACK_BUTTON, constants.COURSES_CALLBACK)
        ]
        reply_markup = create_inline_keyboard(keyboard_data, lang)
        caption = texts.COURSES[course_name][lang]['text']

        with open(f'static/{texts.COURSES[course_name]["photo_url"]}',
                  'rb') as photo:
            media = InputMediaPhoto(photo)
            query.edit_message_media(media)
            query.edit_message_caption(caption, reply_markup,
                                       parse_mode='HTML')

        user_orm.User(chat_id).edit_field('is_main_message', 0)

    handle_user_lang(update, context, show_course_info)


def delete_user_message(update: Update, context: CallbackContext) -> None:
    """Удалить все сообщения, отправленные пользователем."""
    update.message.delete()


def main() -> None:
    """Запуск бота."""
    load_dotenv()
    TELEGRAM_TOKEN = getenv('TELEGRAM_TOKEN')
    check_token(TELEGRAM_TOKEN)
    updater = Updater(TELEGRAM_TOKEN)
    dispatcher = updater.dispatcher

    try:
        dispatcher.add_handler(CommandHandler(constants.START_COMMAND, start))
        dispatcher.add_handler(CallbackQueryHandler(
            save_lang, pattern=constants.LANG_PATTERN))
        dispatcher.add_handler(CallbackQueryHandler(
            main_menu, pattern=constants.MAIN_MENU_CALLBACK))
        dispatcher.add_handler(CallbackQueryHandler(
            contact_info, pattern=constants.CONTACT_INFO_CALLBACK))
        dispatcher.add_handler(CallbackQueryHandler(
            about_academy, pattern=constants.ACADEMY_DESC_CALLBACK))
        dispatcher.add_handler(CallbackQueryHandler(
            courses, pattern=f'^{constants.COURSES_PATTERN}'))
        dispatcher.add_handler(CallbackQueryHandler(
            course_info, pattern=f'^{constants.COURSE_PATTERN}'))
        dispatcher.add_handler(MessageHandler(Filters.all,
                                              delete_user_message))

        updater.start_polling()
        updater.idle()
    except Exception as e:
        logger.error(f'Произошла ошибка: {e}', exc_info=True)
