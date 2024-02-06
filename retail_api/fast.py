from fastapi import FastAPI
from faker import Faker
import random
import numpy as np

app = FastAPI()
fake = Faker()


office_items = [
    "Desk", "Chair", "Computer", "Printer", "Keyboard",
    "Mouse", "Monitor", "File Cabinet", "Desk Lamp", "Notebook",
    "Pen", "Stapler", "Whiteboard", "Calendar", "Paper Shredder",
    "Scissors", "Tape Dispenser", "Corkboard", "Coffee Maker", "Water Cooler",
    "Headphones", "Phone Charger", "Desk Organizer", "Filing Trays", "Desk Mat",
    "Desk Plant", "Wall Clock", "Calculator", "Shredder", "Ink Cartridges",
    "Post-It Notes", "Bulletin Board", "Fax Machine", "Scanner", "Hole Punch",
    "Desk Fan", "Cable Organizer", "Memo Pad", "Safety Cutter", "Nameplate",
    "Desk Chair Mat", "Trash Can", "Power Strip", "Bookshelf", "Pencil Cup",
    "Desk Drawer Organizer", "Laptop Stand", "Desk Clock", "Paper Clips",
    "Highlighters", "Markers", "Desk Phone", "Laptop Bag", "Briefcase",
    "Desk Mirror", "Badge Holder", "USB Flash Drive", "External Hard Drive",
    "Desk Tray", "Printer Paper", "Envelopes", "Desk Hutch", "Cable Clips",
    "Desk Cable Hole Cover", "Business Cards", "Desk Grommet", "Mail Sorter",
    "Desk Tidy", "Calendar Stand", "Desk Pen Holder", "Desk Pad",
    "Eraser", "Correction Fluid", "Paperweight", "Notebook Stand", "Desk Riser",
    "Printer Stand", "Desk Privacy Panel", "Chair Mat", "Monitor Stand",
    "Desk Lamps", "Filing Cabinets", "Cubicle Walls", "Office Plants",
    "Wall Shelves", "Standing Desk", "Task Light", "Cable Management Box",
    "Waste Bin", "Letter Tray", "Bulletin Boards", "Desk Name Plate",
    "Laptop Cooling Pad", "Desk Cable Organizer", "Wireless Charger"
]

office_prices = {item: round(int(random.uniform(20.0, 500.0)), 2) for item in office_items}

# Define a root `/` endpoint
@app.get('/')
def index():
    return {'ok': True}

@app.get('/retail')
def get_sales(limit:int = 1):
    return {
        fake.uuid4():
            {'user':{
                'user_name':fake.last_name(),
                'department':random.randint(1,96),
                'sexe':random.choice(['m','f']),
                'birth_date':fake.date_of_birth(minimum_age=18)
            },
            'item':{
                'item_name':(i:=random.choice(office_items)),
                'price':office_prices[i],
                'price_payed':round(office_prices[i] * np.random.uniform(0.95,1),2)
            },
            'taxes':random.choices([20,0], [a:=random.random(),1-a])[0],
            'quantity':random.randint(1,3)
            }
    for _ in range(limit)
    }
