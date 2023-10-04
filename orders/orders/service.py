from nameko.events import EventDispatcher
from nameko.rpc import rpc
from nameko_sqlalchemy import DatabaseSession

from orders.exceptions import NotFound
from orders.models import DeclarativeBase, Order, OrderDetail
from orders.schemas import OrderSchema


class OrdersService:
    """
    This class provides methods to manage orders.
    """

    name = 'orders'

    db = DatabaseSession(DeclarativeBase)
    event_dispatcher = EventDispatcher()

    @rpc
    def get_order(self, order_id):
        """
        Get an order by its ID.

        Args:
            order_id (int): The ID of the order to retrieve.

        Returns:
            order: The order data.

        Raises:
        exceptions.NotFound: Raises NotFound in case the order with the given order ID doesn't exist.
        """

        order = self.db.query(Order).get(order_id)

        if not order:
            raise NotFound('Order with id {} not found'.format(order_id))

        return OrderSchema().dump(order).data

    @rpc
    def create_order(self, order_details):
        """
        Creates an order.

        Args:
            order_details (dict): The body containing the order details.

        Returns:
            order: The created order data.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product with the given product ID doesn't exist.
        """

        order = Order(
            order_details=[
                OrderDetail(
                    product_id=order_detail['product_id'],
                    price=order_detail['price'],
                    quantity=order_detail['quantity']
                )
                for order_detail in order_details
            ]
        )
        self.db.add(order)
        self.db.commit()

        order = OrderSchema().dump(order).data

        self.event_dispatcher('order_created', {
            'order': order,
        })

        return order
    
    @rpc
    def list_orders(self):
        """
        List all orders.

        Returns:
            orders (list): A list containing the orders.
        """
        orders = self.db.query(Order).all()

        return OrderSchema(many=True).dump(orders).data

    @rpc
    def update_order(self, order):
        """
        Update an existing order with new details.

        Args:
            order (dict): The ID of the order to retrieve.

        Returns:
            order (dict): The updated order data.
        """

        order_details = {
            order_details['id']: order_details
            for order_details in order['order_details']
        }

        order = self.db.query(Order).get(order['id'])

        for order_detail in order.order_details:
            order_detail.price = order_details[order_detail.id]['price']
            order_detail.quantity = order_details[order_detail.id]['quantity']

        self.db.commit()
        return OrderSchema().dump(order).data

    @rpc
    def delete_order(self, order_id):
        """
        Delete an order by its ID.

        Args:
            order_id (int): The ID of the order to be deleted.

        Returns:
            None
        """

        order = self.db.query(Order).get(order_id)
        self.db.delete(order)
        self.db.commit()
