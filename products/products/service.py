import logging

from nameko.events import event_handler
from nameko.rpc import rpc

from products import dependencies, schemas


logger = logging.getLogger(__name__)


class ProductsService:

    """
    Service for managing products.
    """

    name = 'products'

    storage = dependencies.Storage()

    @rpc
    def get(self, product_id):
        """
        Get product by their respective ID.

        Args:
            product_id (str): The ID of the product to retrieve.

        Returns:
            product (dict): The product info.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product with the specified ID is not found in the storage.
        """

        product = self.storage.get(product_id)
        return schemas.Product().dump(product).data

    @rpc
    def list(self):
        """
        List all products.

        Returns:
            list: A list of all available products.
        """

        products = self.storage.list()
        return schemas.Product(many=True).dump(products).data

    @rpc
    def create(self, product):
        """
        Creates a new product.

        Args:
            product (dict): The product data to create.

        Returns:
            id (str): The id of the created product.

        Raises:
        exceptions.ProductExists: Raises ProductExists in case the product id already exists.
        """

        product_data = schemas.Product(strict=True).load(product).data
        self.storage.create(product_data)
        
    @rpc
    def update(self, product, product_id):
        """
        Updates a product.

        Args:
            product (dict): The product data to update.

        Returns:
            product (dict): The updated product data.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product does not exist.
        """

        self.storage.update(product, product_id)

    @rpc
    def delete(self, product_id):
        """
        Deletes a product.

        Args:
            product_id (str): The product ID to delete.

        Returns:
            A message containing the success of the delete transaction.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product does not exist.
        """

        self.storage.delete(product_id)

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        """
        Event handler for order creation. This method is automatically called when an order is created.

        Args:
            payload (dict): The payload containing information about the order to create.

        For each order detail in the payload, this method decrements the stock of the corresponding product by the specified quantity.
        """
        for product in payload['order']['order_details']:
            self.storage.decrement_stock(
                product['product_id'], product['quantity'])
