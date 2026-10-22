import asyncio
import aiohttp
import json
import time
from datetime import datetime

MAKES_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getallmakes?format=json" # все марки
MODELS_API_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/getmodelsformake/{make}?format=json" # конкретная марка

TIMEOUT = 5
MAX_CONCURRENT_REQUESTS = 3
SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def fetch_data(url, session, retries=3):

    async with SEMAPHORE:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        print(f"[OK] {url} (попытка {attempt})")
                        return data
                    else:
                        print(f"[Ошибка {response.status}] {url}")
                        return None

            except asyncio.TimeoutError:
                print(f"[Таймаут] {url} (попытка {attempt}/{retries})")
            except aiohttp.ClientError as e:
                print(f"[Ошибка соединения] {url}: {e} (попытка {attempt}/{retries})")
            except Exception as e:
                print(f"[Неизвестная ошибка] {url}: {e}")

            if attempt < retries:
                await asyncio.sleep(1)

        print(f"[Провал] Не удалось получить данные: {url}")
        return None

async def fetch_with_timeout(url, session, timeout=TIMEOUT):
    try:
        return await asyncio.wait_for(
            fetch_data(url, session),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        print(f"[Таймаут wait_for] {url}")
        return None

async def get_car_data(make_name="honda"):
    async with aiohttp.ClientSession() as session:
        models_url = MODELS_API_URL.format(make=make_name)
        print(f"\n{'='*50}")
        print(f"Запуск запросов для марки: {make_name.upper()}")
        print(f"{'='*50}")
        makes_task = fetch_with_timeout(MAKES_API_URL, session)
        models_task = fetch_with_timeout(models_url, session)
        makes_data, models_data = await asyncio.gather(makes_task, models_task)
        result = {
            "timestamp": datetime.now().isoformat(),
            "make_query": make_name,
            "makes_count": len(makes_data.get("Results", [])) if makes_data else 0,
            "models": [],
            "models_count": 0,
            "errors": []
        }
  
        if models_data and "Results" in models_data:
            result["models"] = [
                {"make": item.get("Make_Name"), "model": item.get("Model_Name")}
                for item in models_data["Results"][:20]
            ]
            result["models_count"] = len(models_data["Results"])
        else:
            result["errors"].append(f"Не удалось получить модели для {make_name}")

        if not makes_data:
            result["errors"].append("Не удалось получить список марок")

        return result
def save_data(data, filename="cars_data.json"):
    try:
        with open(filename, "a", encoding="utf-8") as file:
            file.write(json.dumps(data, ensure_ascii=False) + "\n")
        print(f"[Сохранено] {filename}")
    except Exception as e:
        print(f"[Ошибка сохранения] {e}")

def save_json_pretty(data, filename="cars_data_pretty.json"):
    try:
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
        print(f"[Сохранено] {filename}")
    except Exception as e:
        print(f"[Ошибка сохранения] {e}")
async def periodic_collection(interval_seconds=600, makes=None):
    if makes is None:
        makes = ["honda", "toyota", "ford"]

    iteration = 0
    while True:
        iteration += 1
        print(f"\n{'#'*60}")
        print(f"# ИТЕРАЦИЯ {iteration} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*60}")
        tasks = [get_car_data(make) for make in makes]
        results = await asyncio.gather(*tasks)

        for result in results:
            save_data(result)

        print(f"\n[Ожидание] Следующий сбор через {interval_seconds} сек...")
        await asyncio.sleep(interval_seconds)

async def main():

    print("=" * 60)
    print("АСИНХРОННЫЙ СБОР ДАННЫХ ОБ АВТОМОБИЛЯХ")
    print("=" * 60)

    start_time = time.time()
    makes = ["honda", "toyota", "ford"]
    tasks = [get_car_data(make) for make in makes]
    results = await asyncio.gather(*tasks)
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ")
    print("=" * 60)
    for result in results:
        print(f"\nМарка: {result['make_query'].upper()}")
        print(f"  Всего марок в API: {result['makes_count']}")
        print(f"  Моделей найдено: {result['models_count']}")
        print(f"  Первые модели: {[m['model'] for m in result['models'][:5]]}")
        if result["errors"]:
            print(f"  Ошибки: {result['errors']}")
    save_json_pretty(results, "cars_data_all.json")
    for result in results:
        save_data(result)
    elapsed = time.time() - start_time
    print(f"\n[Время выполнения] {elapsed:.2f} сек")
    print(f"[Всего запросов] {len(makes) * 2}")

if __name__ == "__main__":
    asyncio.run(main())