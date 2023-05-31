from dataiku.scenario import Trigger
from datetime import date
from datetime import datetime

t = Trigger()
aujourdhui = date.today()
heure = datetime.today()
heure_actuelle = heure.hour

if aujourdhui.weekday() >= 0 and aujourdhui.weekday() < 5:
    if heure_actuelle > 8 and heure_actuelle < 21:
        t.fire()
