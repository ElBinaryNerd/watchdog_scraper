import random
import time

class Proxy:
    def __init__(self, proxy_address):
        self.proxy_address = proxy_address
        self.success_count = 0
        self.failure_count = 0
        self.total_load_time = 0
        self.requests_made = 0
        self.penalty_points = 0  # Used for penalizing slow/failing proxies
        self.in_cooldown = False
        self.cooldown_end_time = 0  # Timestamp when cooldown ends

    @property
    def average_load_time(self):
        if self.requests_made > 0:
            return self.total_load_time / self.requests_made
        return float('inf')  # Infinite if no requests made yet

    def get_address(self):
        return self.proxy_address

    def record_request(self, load_time, success, alpha=0.1):
        # Exponentially weighted moving average for load time
        if self.requests_made > 0:
            self.total_load_time = (1 - alpha) * self.total_load_time + alpha * load_time
        else:
            self.total_load_time = load_time  # First request sets the load time
        
        self.requests_made += 1

        if success:
            self.success_count += 1
            self.penalty_points = max(0, self.penalty_points - 1)  # Reduce penalty on success
        else:
            self.failure_count += 1
            self.penalty_points += 1  # Increase penalty on failure

class ProxyManager:
    def __init__(self, proxy_list):
        self.proxies = [Proxy(proxy) for proxy in proxy_list]

    def get_proxy(self):
        available_proxies = [proxy for proxy in self.proxies if not proxy.in_cooldown or time.time() > proxy.cooldown_end_time]

        if not available_proxies:
            raise Exception("No proxies available at the moment")

        # Ensure that the minimum weight is positive
        weights = [max(1e-3, (1 / (proxy.average_load_time + 1)) - proxy.penalty_points) for proxy in available_proxies]

        if sum(weights) <= 0:
            raise Exception("Total of weights must be greater than zero")

        return random.choices(available_proxies, weights=weights, k=1)[0]

    def update_load_time(self, proxy, load_time, success=True):
        # Update load time and success/failure counts
        proxy.record_request(load_time, success)

        # If the proxy's penalty exceeds a certain threshold, apply cooldown
        if proxy.penalty_points > 5:  # Testing this threshold
            proxy.in_cooldown = True
            proxy.cooldown_end_time = time.time() + 300  # 5-minute cooldown

    def decay_penalty_points(self):
        # Gradually reduce penalty points for all proxies
        for proxy in self.proxies:
            proxy.penalty_points = max(0, proxy.penalty_points - 1)

    def cooldown_check(self):
        # Called once per minute to reintegrate proxies that are out of cooldown
        current_time = time.time()
        for proxy in self.proxies:
            if proxy.in_cooldown and current_time > proxy.cooldown_end_time:
                proxy.in_cooldown = False  # Re-enable the proxy

    def show_proxies(self):
        result = ""
        for proxy in self.proxies:
            result = (f"----{proxy.get_address()} | requests: {proxy.requests_made} (s/f: {proxy.success_count} | {proxy.failure_count}\n penalty pts: {proxy.penalty_points} | cooldown? {proxy.in_cooldown} | total load time: {proxy.total_load_time:.1f} s.")
        return result
