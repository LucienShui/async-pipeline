# Async Pipeline

## Usage

```python
from async_pipeline import Pipeline, Item


def plus(item: Item):
    return item.value + 1


def multiply(item: Item):
    return item.value * 3


total = 16
pipeline = Pipeline.from_config([
    {"n": 1, "worker": {"func": plus}},
    {"func": multiply},
], total)
pipeline.start()  # start to listen
for i in range(total):
    pipeline(i)  # feed data into input queue
pipeline.stop()  # run until all data processed

while not pipeline.empty():  # read result from output queue
    print(pipeline.get().value)
```
