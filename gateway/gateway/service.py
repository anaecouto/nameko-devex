import json

from marshmallow import ValidationError
from nameko import config
from nameko.exceptions import BadRequest
from nameko.rpc import RpcProxy
from werkzeug import Response

from gateway.entrypoints import http
from gateway.exceptions import OrderNotFound, ProductNotFound, ProductNotInStock, ProductExists
from gateway.schemas import CreateOrderSchema, GetOrderSchema, ProductSchema, UpdateProductSchema


class GatewayService(object):
    """
    Service acts as a gateway to other services over http.
    """

    name = 'gateway'

    orders_rpc = RpcProxy('orders')
    products_rpc = RpcProxy('products')

    @http(
        "GET", "/products/<string:product_id>",
        expected_exceptions=ProductNotFound
    )
    def get_product(self, request, product_id):
        """Gets product by `product_id`
        """
        product = self.products_rpc.get(product_id)
        return Response(
            ProductSchema().dumps(product).data,
            mimetype='application/json'
        )

    @http(
        "GET", "/products"
    )
    def get_products_list(self, request):
        """Gets a list of products
        """
        products = self.products_rpc.list()
        return Response(
            ProductSchema().dumps(products, many=True).data,
            mimetype='application/json'
        )    

    @http(
        "POST", "/products",
        expected_exceptions=(ValidationError, BadRequest, ProductExists)
    )
    def create_product(self, request):
        """Create a new product - product data is posted as json

        Example request ::

            {
                "id": "the_odyssey",
                "title": "The Odyssey",
                "passenger_capacity": 101,
                "maximum_speed": 5,
                "in_stock": 10
            }


        The response contains the new product ID in a json document ::

            {"id": "the_odyssey"}

        """

        schema = ProductSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.      
            product_data = schema.loads(request.get_data(as_text=True)).data
            self.products_rpc.create(product_data)
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))
        except ProductExists as error:
            raise ProductExists("Failed to create product: {}".format(error))
        return Response(
            json.dumps({'id': product_data['id']}), mimetype='application/json',
            status=201
        )

    @http(
        "PATCH", "/products/<string:product_id>",
        expected_exceptions=(ValidationError, BadRequest, ProductNotFound)
    )
    def update_product(self, request, product_id):  

        schema = UpdateProductSchema(strict=True)
 
        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            product_data = schema.loads(request.get_data(as_text=True)).data

            self.products_rpc.update(product_data, product_id)
            updated_product = self.products_rpc.get(product_id)
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))
        except ProductNotFound as error:
            error_message = "Failed to update product: {}".format(error)
            raise ProductNotFound(error_message)
        
        return Response(
            json.dumps({'message': f'Product with ID {product_id} updated successfully',
                         'updated_product': updated_product}), mimetype='application/json'
        )

    @http(
        "DELETE", "/products/<string:product_id>",
        expected_exceptions=ProductNotFound
    )
    def delete_product(self, request, product_id):
        """Deletes product by `product_id`
        """
        try:
            self.products_rpc.delete(product_id)
        except ProductNotFound as error:
            raise ProductNotFound("Failed to update product: {}".format(error))
        return Response(
            json.dumps({ 'message': f'Product with id {product_id} deleted successfully.' }), 
            mimetype='application/json'
        )
        
    
    @http("GET", "/orders/<int:order_id>", expected_exceptions=(OrderNotFound, ProductNotFound))
    def get_order(self, request, order_id):
        """Gets the order details for the order given by `order_id`.

        Enhances the order details with full product details from the
        products-service.
        """

        try:
            order = self._get_order(order_id)
        except OrderNotFound as error:
            raise OrderNotFound("Failed to get order: {}".format(error))
        except ProductNotFound as error:
            raise ProductNotFound("Failed to get order: {}".format(error))
        return Response(
            GetOrderSchema().dumps(order).data,
            mimetype='application/json'
        )
    
    @http("GET", "/orders/")
    def list_orders(self, request):
        orders = self.orders_rpc.list_orders()
        
        return Response(
            GetOrderSchema(many=True).dumps(orders).data,
            mimetype='application/json'
        )
    

    def _get_order(self, order_id):
        # Retrieve order data from the orders service.
        # Note - this may raise a remote exception that has been mapped to
        # raise``OrderNotFound``
        order = self.orders_rpc.get_order(order_id)

        # get the configured image root
        image_root = config['PRODUCT_IMAGE_ROOT']

        # Enhance order details with product and image details
        for item in order['order_details']:
            product_id = item['product_id']
            # Construct an image url.
            item['image'] = '{}/{}.jpg'.format(image_root, product_id)

        return order

    @http(
        "POST", "/orders",
        expected_exceptions=(ValidationError, ProductNotFound, BadRequest, ProductNotInStock)
    )
    def create_order(self, request):
        """Create a new order - order data is posted as json

        Example request ::

            {
                "order_details": [
                    {
                        "product_id": "the_odyssey",
                        "price": "99.99",
                        "quantity": 1
                    },
                    {
                        "price": "5.99",
                        "product_id": "the_enigma",
                        "quantity": 2
                    },
                ]
            }


        The response contains the new order ID in a json document ::

            {"id": 1234}

        """

        schema = CreateOrderSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            order_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Create the order
        # Note - this may raise `ProductNotFound or ProductNotInStock`
        try:
            id_ = self._create_order(order_data)
        except ProductNotInStock as error:
            raise ProductNotInStock("Failed to create order: {}".format(error))
        except ProductNotFound as error:
            raise ProductNotFound("Failed to create order: {}".format(error))
        
        return Response(json.dumps({'id': id_}), mimetype='application/json')

    def _create_order(self, order_data):

        all_products = self.products_rpc.list()

        valid_product_ids = set()
        product_in_stock = {}
    
        for prod in all_products:
            valid_product_ids.add(prod['id'])
            product_in_stock[prod['id']] = prod['in_stock']
    
        for item in order_data['order_details']:
            product_id = item['product_id']
            if product_id not in valid_product_ids:
                raise ProductNotFound(
                    "Product ID {} does not exist".format(product_id)
                )
            if product_in_stock.get(product_id, 0) == 0:
                raise ProductNotInStock(
                    "Product with ID {} is not in stock".format(product_id)
                )
            if item['quantity'] > product_in_stock.get(product_id, 0):
                raise BadRequest("Order quantity exceeds quantity limit.")

        # Call orders-service to create the order.
        # Dump the data through the schema to ensure the values are serialized
        # correctly.
        serialized_data = CreateOrderSchema().dump(order_data).data
        result = self.orders_rpc.create_order(
            serialized_data['order_details']
        )
        return result['id']
