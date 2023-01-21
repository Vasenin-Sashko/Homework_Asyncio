import asyncio
from datetime import datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

CHUNK_SIZE = 10
PG_DSN = 'postgresql+asyncpg://app:12345@localhost:5432/app'
engine = create_async_engine(PG_DSN)
Base = declarative_base()
async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


async def get_deep_url(url, key, session):
    async with session.get(f'{url}') as response:
        data = await response.json()
        return data[key]


async def get_deep_urls(urls, key, session):
    tasks = (asyncio.create_task(get_deep_url(url, key, session)) for url in urls)
    for task in tasks:
        yield await task


async def get_data(urls, key, session):
    result_list = []
    async for item in get_deep_urls(urls, key, session):
        result_list.append(item)
    return ', '.join(result_list)


async def insert_people(people_chunk):
    async with async_session_maker as orm_session:
        async with ClientSession() as data_session:
            for person_json in people_chunk:
                if person_json.get('status') == 404:
                    break
                homeworld_str = await get_data([person_json['homeworld']], 'name', data_session)
                films_str = await get_data(person_json['films'], 'title', data_session)
                species_str = await get_data(person_json['species'], 'name', data_session)
                starships_str = await get_data(person_json['starships'], 'name', data_session)
                vehicles_str = await get_data(person_json['vehicles'], 'name', data_session)
                newperson = People(
                    birth_year=person_json['birth_year'],
                    eye_color=person_json['eye_color'],
                    gender=person_json['gender'],
                    hair_color=person_json['hair_color'],
                    height=person_json['height'],
                    mass=person_json['mass'],
                    name=person_json['name'],
                    skin_color=person_json['skin_color'],
                    homeworld=homeworld_str,
                    films=films_str,
                    species=species_str,
                    starships=starships_str,
                    vehicles=vehicles_str,
                )
                orm_session.add(newperson)
                await orm_session.commit()


async def get_person(person_id: int, session: ClientSession):
    print(f'begin {person_id}')
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        if response.status == 404:
            return {'status': 404}
        person = await response.json()
        print(f'end {person_id}')
        return person


async def get_people():
    async with ClientSession() as session:
        for id_chunk in chunked(range(1, 200), CHUNK_SIZE):
            coros = [get_person(i, session=session) for i in id_chunk]
            people = await asyncio.gather(*coros)
            for item in people:
                yield item


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async for chunk in chunked_async(get_people(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))
    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


if __name__ == '__main__':
    start = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start)
