import logging

from app.broker import consumer, decode
from app.database import connect, save


logging.basicConfig(level=logging.INFO)


def main():
    source = consumer("scores", "scores-postgres")
    try:
        with connect() as connection:
            for message in source:
                result = decode(message)
                save(connection, result)
                source.commit()
                logging.info("Сохранена транзакция %s", result["transaction_id"])
    finally:
        source.close()


if __name__ == "__main__":
    main()
