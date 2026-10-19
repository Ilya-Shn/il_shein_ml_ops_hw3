import json
import logging
import time

from app.broker import consumer, decode, producer
from app.scorer import Scorer


logging.basicConfig(level=logging.INFO)


def main():
    scorer = Scorer()
    while not scorer.models.joinpath("state.json").exists():
        time.sleep(2)
    while not json.loads(scorer.models.joinpath("state.json").read_text())["active"]:
        time.sleep(2)
    source = consumer("transactions", "fraud-scorer")
    output = producer()
    try:
        for message in source:
            try:
                result = scorer.score(decode(message))
            except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
                output.send("errors", {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "error": str(error),
                }).get(timeout=30)
                logging.warning("Сообщение отправлено в errors: %s", error)
            else:
                output.send("scores", key=result["transaction_id"].encode(), value=result).get(timeout=30)
                logging.info("Обработана транзакция %s", result["transaction_id"])
            source.commit()
    finally:
        output.close()
        source.close()


if __name__ == "__main__":
    main()
