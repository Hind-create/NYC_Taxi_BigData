from pathlib import Path
from urllib.request import urlopen

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
DESTINATION = Path("data/yellow_tripdata_2025-01.parquet")


def main():
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)

    if DESTINATION.exists():
        print(f"Le fichier existe déjà : {DESTINATION}")
        print("Supprime-le manuellement si tu souhaites le télécharger à nouveau.")
        return

    print(f"Téléchargement depuis : {URL}")
    print(f"Destination : {DESTINATION}")

    with urlopen(URL, timeout=60) as response, DESTINATION.open("wb") as output:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)

    print(f"Téléchargement terminé : {DESTINATION.stat().st_size:,} octets")


if __name__ == "__main__":
    main()
