import pandas as pd
import uuid
import random
from faker import Faker
import datetime

def FaceInPage():
    # https://towardsdatascience.com/build-a-your-own-custom-dataset-using-python-9296540a0178 source
    num_users = 1000
    features = [
        "ID",
        "Name",
        "Nationality",
        "CountryCode",
        "Hobby"
    ]
    face_in_page = pd.DataFrame(columns=features)

    # generating ids
    face_in_page['ID'] = [(i+1) for i in range(num_users)]

    # generating names
    faker = Faker()
    face_in_page['Name'] = [faker.name() for i in range(num_users)]

    # generating nationality
    nationalities = pd.read_csv("CH_Nationality_List_20171130_v1.csv")
    nationalities_array = nationalities.to_numpy(dtype=str)
    face_in_page['Nationality'] = [nationalities_array[random.randint(0, 224)][0] for i in range(num_users)]
    print(face_in_page['Nationality'])

    # generating country code


    # generating hobbies


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    FaceInPage()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
