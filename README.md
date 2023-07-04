# Async Pipeline

## Usage

```python
from async_pipeline import Pipeline, Item


def plus(item: Item):
    return item.value + 1

def multiply(item: Item):
    return item.value * 3


total = 16
pipeline = Pipeline([(plus, 1), (multiply, 1)], total=total)
pipeline.start()  # start to listen
for i in range(total):
    pipeline(i)  # feed data into input queue
pipeline.stop()  # run until all data processed

while not pipeline.empty():  # read result from output queue
    print(pipeline.get().value)
```
