import logging

from nameko.events import event_handler
from nameko.rpc import rpc

from products import dependencies, schemas, exceptions


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
            product_id (varchar): The ID of the product to retrieve.

        Returns:
            dict: The product info.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product with the specified ID is not found in the storage.
        """

        try:
            product = self.storage.get(product_id)
            return schemas.Product().dump(product).data
        except exceptions.NotFound as error:
            logger.error(f"{error}")
            raise error

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
            The created product data.

        Raises:
        exceptions.ProductExists: Raises ProductExists in case the product id already exists.
        """

        try:
            product_data = schemas.Product(strict=True).load(product).data
            product_exists = self.storage.get(product_data['id'])
            
            if product_exists:
                error_message = "Product with this ID already exists"
                logger.error(error_message)

                raise exceptions.ProductExists(error_message)
        except exceptions.NotFound:
            self.storage.create(product)
        
    @rpc
    def update(self, product, product_id):
        """
        Updates a product.

        Args:
            product (dict): The product data to update.

        Returns:
            The updated product data.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product does not exist.
        """

        self.storage.update(product, product_id)

    @rpc
    def delete(self, product_id):
        """
        Deletes a product.

        Args:
            product_id (varchar): The product ID to delete.

        Returns:
            A message containing the success of the delete transaction.

        Raises:
        exceptions.NotFound: Raises NotFound in case the product does not exist.
        """

        self.storage.delete(product_id)

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload):
        for product in payload['order']['order_details']:
            self.storage.decrement_stock(
                product['product_id'], product['quantity'])
