"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread, current_thread
from time import sleep


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        super().__init__(**kwargs)
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time

    def run(self):
        for cart in self.carts:
            cart_id = self.marketplace.new_cart()
            for operation in cart:
                for _ in range(operation['quantity']):
                    if operation['type'] == 'add':
                        while not self.marketplace.add_to_cart(cart_id, operation['product']):
                            sleep(self.retry_wait_time)
                    elif operation['type'] == 'remove':
                        self.marketplace.remove_from_cart(cart_id, operation['product'])
            products = self.marketplace.place_order(cart_id)
            for product in products:
                print(f"{current_thread().name} bought {product}")
