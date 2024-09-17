"""Order views."""
from django.shortcuts import get_object_or_404
from rest_framework import mixins, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import GenericViewSet, ModelViewSet

from ..serializers.order_serializers import (
    AddressSerializer, CartSerializer, CartItemSerializer, OrderSerializer)
from core.shortcuts import get_current_user_cart
from order.models import Address, Order


class OrderViewSet(mixins.ListModelMixin, mixins.CreateModelMixin,
                   mixins.RetrieveModelMixin, GenericViewSet):
    """Order Viewset."""

    serializer_class = OrderSerializer
    permission_classes = (IsAuthenticated,)

    def get_queryset(self):
        """Get queryset to work with."""
        return Order.objects.filter(user=self.request.user)

    def get_serializer_context(self):
        """Return the context for the serializer."""
        context = super().get_serializer_context()
        context.update({
            'user': self.request.user,
        })
        return context

    @action(detail=True, methods=['post'], url_path='cancel',
            url_name='cancel-order')
    def cancel_order(self, request, pk=None):
        """Handle canceling an order."""
        order = get_object_or_404(Order, pk=pk, user=request.user)
        order.status = Order.OrderStatuses.CANCELLED
        order.save()
        return Response({'status': 'order canceled'},
                        status=status.HTTP_200_OK)


class CartItemViewSet(mixins.RetrieveModelMixin, mixins.CreateModelMixin,
                      mixins.UpdateModelMixin, mixins.DestroyModelMixin,
                      GenericViewSet):
    """CartItem view set."""

    serializer_class = CartItemSerializer
    permission_classes = (IsAuthenticated,)

    def get_queryset(self):
        """Get queryset to work with."""
        cart = get_current_user_cart(self.request.user)
        return cart.cart_items.all()

    def get_serializer_context(self):
        """Return the context for the serializer."""
        context = super().get_serializer_context()
        context.update({
            'user': self.request.user,
            'action': self.action
        })
        return context


class CartApiView(APIView):
    """Cart api view."""

    permission_classes = (IsAuthenticated,)

    def get(self, request) -> Response:
        """Return current items in the cart.

        You should just send the query without anything. Cart
        will be found by requested user automaticly.
        """
        cart = get_current_user_cart(request.user)
        serializer = CartSerializer(cart)
        return Response(serializer.data, status=status.HTTP_200_OK)


class AddressViewSet(ModelViewSet):
    """Address viewset."""

    permission_classes = (IsAuthenticated,)
    serializer_class = AddressSerializer

    def get_queryset(self):
        """Return queryset to work with."""
        return Address.objects.filter(user=self.request.user)

    def delete(self, request, pk=None):
        """Unsubscribe from the user."""
        queryset = self.get_queryset()
        obj = get_object_or_404(queryset, pk=pk)
        obj.user = None
        obj.save()
        return Response({'status': 'Successfully delted'}, status.HTTP_200_OK)
