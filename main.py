from azure.functions import App
from . import blueprint

app = App()
app.register_functions(blueprint.blueprint)
