from faker import Faker
from random import randint, choice
from datetime import datetime, timedelta
import pandas as pd

import secrets

def generate_alphanumeric_code(length=5):
    return ''.join(secrets.choice('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(length)).upper()


# Create a Faker instance
fake = Faker()

def create_fake_clients(n):
    clients = []
    for _ in range(n):
        days_past = randint(0, 365*10)
        onboarding_date = datetime.now() - timedelta(days=days_past)
        aum = randint(10000, 100000000)
        intrnal_id = generate_alphanumeric_code()
        product_type = choice(['Master Trust', 'Own Trust', 'GPP'])
        contributions = randint(1000, 10000)
        revenue = randint(1000, 10000)
        client_representative = fake.name()
        active_members = randint(1, 10000)
        deferred_members = randint(1, 10000)

        client = {
            'internal_id': intrnal_id,
            'external_id': fake.unique.random_number(digits=5),
            'company': fake.company(),
            'product_type': product_type,
            'status': choice(['Active', 'Lost', 'Internal', 'Prospect']),
            'client_representative': client_representative,
            'contact_name': fake.name(),
            'business_type': choice(['Financial Services', 'Technology', 'Healthcare', 'Retail', 'Manufacturing']),
            'website': fake.url(),
            'description': fake.text(),
            'city': fake.city(),
            'email': fake.email(),
            'onboarding_date': onboarding_date,
            'aum': aum,
            'contributions': contributions,
            'revenue': revenue,
            'active_members': active_members,
            'deferred_members': deferred_members,
            'created_at': datetime.now(),

        }
        clients.append(client)
    return clients

# Generate fake clients
clients = create_fake_clients(500)



df = pd.DataFrame(clients)
df.to_csv('client_database/data/client_database.csv', index=False)