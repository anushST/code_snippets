"""Views of users app."""
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.shortcuts import get_object_or_404
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import permissions, status, viewsets
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.permissions import AllowAny
from rest_framework_simplejwt.tokens import RefreshToken

from .serializers import (
    LoginSerializer, RegisterSerializer, RefreshTokenSerializer,
    UserSerializer)

User = get_user_model()


def get_tokens_for_user(user):
    """Generate refresh and access JWT tokens for the given user.

    This function creates a refresh token and an access token for the specified
    Django user instance. The tokens are generated using the RefreshToken class
    from the Simple JWT package.

    **Params**:
        - `user`: Required. The user object.

    **Returns dictionary with keys**:
        - 'refresh': A string representing the refresh token.
        - 'access': A string representing the access token.
    """
    refresh = RefreshToken.for_user(user)
    return {
        'refresh': str(refresh),
        'access': str(refresh.access_token),
    }


class LoginAPIView(APIView):
    """API view for user login that returns JWT tokens."""

    @swagger_auto_schema(
        request_body=LoginSerializer,
        responses={
            200: openapi.Response('Success', schema=openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties={
                    'access': openapi.Schema(type=openapi.TYPE_STRING),
                    'refresh': openapi.Schema(type=openapi.TYPE_STRING),
                }
            )),
            400: 'Bad Request',
            401: 'Invalid Credentials',
        },
        tags=['Auth']
    )
    def post(self, request):
        """Authenticate user and return JWT tokens.

        This endpoint allows users to log in with either their
        username or email
        and password. If the credentials are valid, it returns
        access and refresh
        tokens.

        **Request body**:
        - `username_or_email`: Required. Username or email of the user.
        - `password`: Required. Password of the user.

        **Responses**:
        - 200: Returns JWT tokens if authentication is successful.
        - 400: Returns an error if the request body is invalid.
        - 401: Returns an error if the credentials are invalid.
        """
        serializer = LoginSerializer(data=request.data)

        if serializer.is_valid():
            username_or_email = serializer.validated_data['username_or_email']
            password = serializer.validated_data['password']

            user = get_object_or_404(
                User,
                Q(email=username_or_email) | Q(username=username_or_email)
            )

            if user.check_password(password):
                tokens = get_tokens_for_user(user)
                return Response(tokens, status=status.HTTP_200_OK)

            return Response({"detail": "Invalid credentials"},
                            status=status.HTTP_401_UNAUTHORIZED)

        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


class RefreshTokenAPIView(APIView):
    """API view for refreshing access tokens using a valid refresh token."""

    permission_classes = [AllowAny]

    @swagger_auto_schema(
        request_body=RefreshTokenSerializer,
        responses={
            200: openapi.Response('Access token', openapi.Schema(
                type=openapi.TYPE_OBJECT,
                properties={
                    'access': openapi.Schema(type=openapi.TYPE_STRING,
                                             description='Access token'),
                }
            )),
            400: openapi.Response('Refresh token is required'),
            401: openapi.Response('Invalid refresh token'),
        },
        tags=['Auth']
    )
    def post(self, request):
        """Generate a new access token from a valid refresh token.

        This endpoint allows users to refresh their access token
        by providing a valid refresh token. If the refresh token
        is valid, a new access token will be returned.

        **Request body**:
        - `refresh`: Required. The refresh token to be validated.

        **Responses**:
        - 200: Returns a new access token if the refresh token is valid.
        - 400: Returns an error if the refresh token is not provided.
        - 401: Returns an error if the refresh token is invalid.
        """
        serializer = RefreshTokenSerializer(data=request.data)

        serializer.is_valid(raise_exception=True)
        refresh_token = serializer.validated_data['refresh']

        try:
            refresh = RefreshToken(refresh_token)
            access_token = refresh.access_token
            return Response({'access': str(access_token)},
                            status=status.HTTP_200_OK)
        except Exception:
            return Response({"detail": "Invalid refresh token"},
                            status=status.HTTP_401_UNAUTHORIZED)


class RegisterView(APIView):
    """API endpoint for user registration."""

    permission_classes = (AllowAny,)

    @swagger_auto_schema(
        request_body=RegisterSerializer,
        responses={
            201: openapi.Response('Registration Successful',
                                  RegisterSerializer),
            400: 'Bad Request'
        },
        operation_description="User registration with email verification",
        operation_summary="Register a new user",
        tags=['Auth']
    )
    def post(self, request):
        """Register a new user and send a confirmation email.

        This endpoint allows new users to register by providing their
        email, username, and password. Upon successful registration,
        a confirmation email is sent to the user with a verification link.

        **Request body**:
        - `email`: Required. The user's email address.
        - `username`: Required. The desired username for the user.
        - `password`: Required. The password for the user account.

        **Responses**:
        - 201: Returns JWT tokens upon successful registration.
        - 400: Returns an error if the registration data is invalid.
        """
        serializer = RegisterSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        user = serializer.save()
        tokens = get_tokens_for_user(user)
        return Response(tokens, status=status.HTTP_201_CREATED)


class VerifyEmailView(APIView):
    """API endpoint for email verification."""

    permission_classes = (AllowAny,)

    @swagger_auto_schema(
        manual_parameters=[openapi.Parameter(
            'token', openapi.IN_PATH,
            description="Token for email verification",
            type=openapi.TYPE_STRING)],
        responses={
            200: 'Email confirmed successfully',
            400: 'Invalid or expired token',
            409: 'Email already confirmed',
        },
        operation_description="Verify email with a confirmation token",
        operation_summary="Email verification",
        tags=['Auth']
    )
    def get(self, request, token):
        """Verify a user's email by validating the provided token.

        This endpoint allows users to confirm their email address
        by visiting a confirmation link sent to their email after
        registration. If the token is valid, the user's account is activated.

        **Parameters**:
        - `token`: Required. The confirmation token sent to the user's email.

        **Responses**:
        - 200: Returns a success message if the email is confirmed.
        - 400: Returns an error if the token is invalid or expired.
        - 409: Returns an error if the email is already confirmed.
        """
        try:
            data = RegisterSerializer.decode_confirmation_token(token)
            user = User.objects.get(id=data['user_id'])
            if not user.is_active:
                user.is_active = True
                user.save()
                return Response({'message': 'Email confirmed'},
                                status=status.HTTP_200_OK)
            return Response({'message': 'Email already confirmed'},
                            status=status.HTTP_409_CONFLICT)
        except Exception:
            return Response({'error': 'Invalid or expired token'},
                            status=status.HTTP_400_BAD_REQUEST)


class UserViewSet(viewsets.ViewSet):
    """ViewSet for managing user accounts."""

    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_description="Retrieve the current user's data.",
        responses={
            200: openapi.Response('User data retrieved successfully',
                                  UserSerializer),
            401: 'Unauthorized',
        },
        operation_summary="Get current user data",
        tags=['Users']
    )
    @action(detail=False, methods=('get',),
            url_path='me', url_name='current_user')
    def current_user(self, request):
        """Get the current user's data.

        This endpoint retrieves the data of the currently authenticated user.

        **Responses**:
        - 200: Returns the user's data if the request is successful.
        - 401: Returns an error if the user is not authenticated.
        """
        user = request.user
        serializer = UserSerializer(user)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        method='put',
        request_body=UserSerializer,
        operation_description="Update the current user's data using PUT.",
        responses={
            200: openapi.Response('User data updated successfully',
                                  UserSerializer),
            400: 'Bad Request',
            401: 'Unauthorized',
        },
        operation_summary="Update current user data (PUT)",
        tags=['Users']
    )
    @swagger_auto_schema(
        method='patch',
        request_body=UserSerializer,
        operation_description=('Partially update the current user'
                               's data using PATCH.'),
        responses={
            200: openapi.Response('User data updated successfully',
                                  UserSerializer),
            400: 'Bad Request',
            401: 'Unauthorized',
        },
        operation_summary="Partially update current user data (PATCH)",
        tags=['Users']
    )
    @action(detail=False, methods=('put', 'patch',),
            url_name='update_user')
    def update_user(self, request, pk):
        """Update the current user's data.

        This endpoint allows the authenticated user to update their
        profile information.

        **Request body**:
        - `first_name`: Optional. The user's new first name.
        - `last_name`: Optional. The user's new last name.

        **Responses**:
        - 200: Returns the updated user data if the request is successful.
        - 400: Returns an error if the update data is invalid.
        - 401: Returns an error if the user is not authenticated.
        """
        user = request.user
        serializer = UserSerializer(user, data=request.data, partial=True)

        serializer.is_valid(raise_exception=True)
        serializer.save()
        return Response(serializer.data, status=status.HTTP_200_OK)
