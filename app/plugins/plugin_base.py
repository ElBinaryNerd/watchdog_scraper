class PluginBase:
    def process(self, html):
        raise NotImplementedError("Subclasses must implement this method")