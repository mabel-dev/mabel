import time
import os
import sys
import random
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from mabel.utils.lru_index import LruIndex
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

STAR_WARS = [
    "Luke Skywalker",
    "C-3PO",
    "R2-D2",
    "Darth Vader",
    "Leia Organa",
    "Owen Lars",
    "Beru Whitesun Lars",
    "R5-D4",
    "Biggs Darklighter",
    "Obi-Wan Kenobi",
    "Anakin Skywalker",
    "Wilhuff Tarkin"
    "Chewbacca",
    "Han Solo",
    "Greedo",
    "Jabba Desilijic Tiure",
    "Wedge Antilles",
    "Jek Tono Porkins",
    "Yoda",
    "Palpatine",
    "Boba Fett",
    "IG-88",
    "Bossk",
    "Lando Calrissian",
    "Lobot",
    "Ackbar",
    "Mon Mothma",
    "Arvel Crynyd",
    "Wicket Systri Warrick"
    "Nien Nunb",
    "Qui-Gon Jinn",
    "Nute Gunray",
    "Finis Valorum",
    "Jar Jar Binks",
    "Roos Tarpals",
    "Rugor Nass",
    "Ric Olié",
    "Watto",
    "Sebulba",
    "Quarsh Panaka",
    "Shmi Skywalker",
    "Darth Maul",
    "Bib Fortuna",
    "Ayla Secura",
    "Dud Bolt",
    "Gasgano",
    "Ben Quadinaros",
    "Mace Windu",
    "Ki-Adi-Mundi",
    "Kit Fisto",
    "Eeth Koth",
    "Adi Gallia",
    "Saesee Tiin",
    "Yarael Poof",
    "Plo Koon",
    "Mas Amedda",
    "Gregar Typho",
    "Cordé",
    "Cliegg Lars",
    "Poggle the Lesser",
    "Luminara Unduli",
    "Barriss Offee",
    "Dormé",
    "Dooku",
    "Bail Prestor Organa",
    "Jango Fett",
    "Zam Wesell",
    "Dexter Jettster",
    "Lama Su",
    "Taun We",
    "Jocasta Nu",
    "Ratts Tyerell",
    "R4-P17",
    "Wat Tambor",
    "San Hill",
    "Shaak Ti",
    "Grievous",
    "Tarfful",
    "Raymus Antilles",
    "Sly Moore",
    "Tion Medon",
    "Finn",
    "Poe Dameron",
    "Captain Phasma",
    "Padmé Amidala",
    "Abafar",
    "Agamar",
    "Ahch-To",
    "Akiva",
    "Alderaan",
    "Aleen",
    "Alzoc",
    "Anaxes",
    "Ando",
    "Anoat",
    "Atollon",
    "Balnab",
    "Batuu",
    "Bespin",
    "Bracca",
    "Cantonica",
    "Castilon",
    "Chandrila",
    "Christophsis",
    "Corellia",
    "Coruscant",
    "Crait",
    "DQar",
    "Dagobah",
    "Dantooine",
    "Dathomir",
    "Devaron",
    "Eadu",
    "Endor",
    "Er´kit",
    "Eriadu",
    "Tarkin",
    "Esseles",
    "Exegol",
    "Felucia",
    "Florrum",
    "Fondor",
    "Geonosis",
    "Hoth",
    "Iego",
    "Ilum",
    "Iridonia",
    "Jakku",
    "Jedha",
    "Jestefad",
    "Kamino",
    "Kashyyyk",
    "Kessel",
    "Kijimi",
    "Kuat",
    "Lothal",
    "Malachor",
    "Malastare",
    "Maridun",
    "Mimban",
    "Moraband",
    "Mortis",
    "Mustafar",
    "Mygeeto",
    "Naboo",
    "Nevarro",
    "Onderon",
    "Pasaana",
    "Pillio",
    "Polis",
    "Rishi",
    "Rodia",
    "Rugosa",
    "Ruusan",
    "Ryloth",
    "Saleucami",
    "Savareen",
    "Scarif",
    "Serenno",
    "Shili",
    "Sissubo",
    "Skako",
    "Sorgan",
    "Subterrel",
    "Sullust",
    "Takodana",
    "Tatooine",
    "Teth",
    "Toydaria",
    "Trandosha",
    "Umbara",
    "Utapau",
    "Vandor-1",
    "Vardos",
    "Wobani",
    "Wrea",
    "Yavin",
    "Zeffo",
    "Zygerria"
]

def lru_performance():

    lru = LruIndex(size=75)

    values = []
    for i in range(1000):
        values.append(random.choice(STAR_WARS))

    start = time.time_ns()

    for i in range(3000):
        for val in values:
            lru.test(val)

    print((time.time_ns() - start) / 1e9)


if __name__ == "__main__":
    lru_performance()

    print('okay')






