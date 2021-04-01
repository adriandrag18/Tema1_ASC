"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer
        self.current_producer_id = -1
        self.producer_queues = {}
        self.producers_lock = Lock()
        self.current_cart_id = -1
        self.carts = {}
        self.consumers_lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.producers_lock:
            self.current_producer_id += 1
            self.producer_queues[self.current_producer_id] = []
            aux = self.current_producer_id
            return aux

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if len(self.producer_queues[producer_id]) >= self.queue_size_per_producer:
            return False

        self.producer_queues[producer_id].append(product)
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.consumers_lock:
            self.current_cart_id += 1
            self.carts[self.current_cart_id] = []
            return self.current_cart_id

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """

        for producer_id, producer_queue in self.producer_queues.items():
            if product in producer_queue:
                producer_queue.remove(product)
                self.carts[cart_id].append((producer_id, product))
                return True
        return False

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        p_id = -1
        for producer_id, cart_product in self.carts[cart_id]:
            if cart_product == product:
                self.producer_queues[producer_id].append(product)
                p_id = producer_id
        self.carts[cart_id].remove((p_id, product))

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        return [cart_product for producer_id, cart_product in self.carts[cart_id]]
