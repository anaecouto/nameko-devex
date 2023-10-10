from os import name
from fastapi import APIRouter, status, HTTPException
from fastapi.params import Depends
from typing import List
from gateapi.api import schemas
from gateapi.api.dependencies import get_rpc, config
from .exceptions import OrderNotFound, ProductNotFound, ProductNotInStock

router = APIRouter(
    prefix = "/orders",
    tags = ['Orders']
)

@router.get("/{order_id}", status_code=status.HTTP_200_OK)
def get_order(order_id: int, rpc = Depends(get_rpc)):
    try:
        return _get_order(order_id, rpc)
    except OrderNotFound as error:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(error)
        )

def _get_order(order_id, nameko_rpc):
    # Retrieve order data from the orders service.
    # Note - this may raise a remote exception that has been mapped to
    # raise``OrderNotFound``
    with nameko_rpc.next() as nameko:
        order = nameko.orders.get_order(order_id)

    # Retrieve all products from the products service
    with nameko_rpc.next() as nameko:
        product_map = {prod['id']: prod for prod in nameko.products.list()}

    # get the configured image root
    image_root = config['PRODUCT_IMAGE_ROOT']

    # Enhance order details with product and image details.
    for item in order['order_details']:
        product_id = item['product_id']

        item['product'] = product_map[product_id]
        # Construct an image url.
        item['image'] = '{}/{}.jpg'.format(image_root, product_id)

    return order

@router.post("", status_code=status.HTTP_200_OK, response_model=schemas.CreateOrderSuccess)
def create_order(request: schemas.CreateOrder, rpc = Depends(get_rpc)):
    try:
        id_ =  _create_order(request.dict(), rpc)
    except ProductNotInStock as error:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
        detail="Failed to create order: {}".format(error))
    except ProductNotFound as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
        detail="Failed to create order: {}".format(error))
    return {
        'id': id_
    }

def _create_order(order_data, nameko_rpc):
    with nameko_rpc.next() as nameko:
        all_products = nameko.products.list()
    
        valid_product_ids = set()
        product_in_stock = {}
        
        for prod in all_products:
            valid_product_ids.add(prod['id'])
            product_in_stock[prod['id']] = prod['in_stock']
        # check order product ids are valid
        for item in order_data['order_details']:
            product_id = item['product_id']
            
            if product_id not in valid_product_ids:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product with id {item['product_id']} not found")
            if product_in_stock.get(product_id, 0) == 0:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                detail="Product with ID {} is not in stock".format(product_id)
                )
            if item['quantity'] > product_in_stock.get(product_id, 0):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                detail="Order quantity exceeds quantity limit.")
            
        # Call orders-service to create the order.
        result = nameko.orders.create_order(
            order_data['order_details']
        )
        return result['id']
    
@router.get("", status_code=status.HTTP_200_OK)
def list_orders(items=1, items_per_page=10, rpc = Depends(get_rpc)):
    with rpc.next() as nameko_rpc:
        return nameko_rpc.orders.list_orders(int(items), int(items_per_page))