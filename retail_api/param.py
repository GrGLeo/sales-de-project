import random

items = [
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

items_prices = {item: (round(int(random.uniform(20.0, 500.0)))+0.99, i+1) for i,item in enumerate(items)}
