
import leip

def dispatch():
    """
    Run the Tui app
    """
    app.run()


app = leip.app(name='tui')
app.discover(globals())
