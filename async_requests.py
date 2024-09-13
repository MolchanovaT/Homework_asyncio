import asyncio
import datetime

import aiohttp
import more_itertools

from models import SessionDB, SwapiPeople, init_orm

MAX_REQUESTS = 5


async def fetch_data(session, url):
    async with session.get(url) as response:
        return await response.json()


async def get_people(person_id, session):
    response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json_data = await response.json()

    films = json_data.get('films')
    films_str = await get_items(films, 'title', session)
    json_data['films'] = films_str

    species = json_data.get('species')
    species_str = await get_items(species, 'name', session)
    json_data['species'] = species_str

    starships = json_data.get('starships')
    starships_str = await get_items(starships, 'name', session)
    json_data['starships'] = starships_str

    vehicles = json_data.get('vehicles')
    vehicles_str = await get_items(vehicles, 'name', session)
    json_data['vehicles'] = vehicles_str

    homeworld = json_data.get('homeworld')
    if homeworld is not None:
        homeworld_json = await fetch_data(session, homeworld)
        json_data['homeworld'] = homeworld_json.get('name')

    return json_data


async def get_items(items_list, name, session):
    if items_list is None or len(items_list) == 0:
        return ''

    coros = []
    for item in items_list:
        coro = fetch_data(session, item)
        coros.append(coro)
    data = await asyncio.gather(*coros)
    titles = []
    for el in data:
        titles.append(el[name])
    return ', '.join(titles)


async def insert_people(people_list):
    async with SessionDB() as session:
        orm_model_list = []
        for person_dict in people_list:
            new_orm_obj = SwapiPeople(
                birth_year=person_dict.get('birth_year'),
                eye_color=person_dict.get('eye_color'),
                films=person_dict.get('films'),
                gender=person_dict.get('gender'),
                hair_color=person_dict.get('hair_color'),
                height=person_dict.get('height'),
                homeworld=person_dict.get('homeworld'),
                mass=person_dict.get('mass'),
                name=person_dict.get('name'),
                skin_color=person_dict.get('skin_color'),
                species=person_dict.get('species'),
                starships=person_dict.get('starships'),
                vehicles=person_dict.get('vehicles')
            )
            orm_model_list.append(new_orm_obj)
        session.add_all(orm_model_list)
        await session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as session_http:
        coros = (get_people(i, session_http) for i in range(1, 101))
        for coros_chunk in more_itertools.chunked(coros, 5):
            people_list = await asyncio.gather(*coros_chunk)
            asyncio.create_task(insert_people(people_list))

        tasks = asyncio.all_tasks()
        main_task = asyncio.current_task()
        tasks.remove(main_task)
        await asyncio.gather(*tasks)


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
