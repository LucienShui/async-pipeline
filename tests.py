import unittest
from async_pipeline import Node, ProcessConsumer, ProgressCenter, Item, Pipeline
from multiprocessing import Queue
import time


def plus(item: Item):
    time.sleep(.2)
    return item.value + 1


def multiply(item: Item):
    time.sleep(.3)
    return item.value * 3


class Plus(ProcessConsumer):
    def process(self, item: Item) -> object:
        time.sleep(.2)
        print(self.thread_id)
        return item.value + 1


class Multiply(ProcessConsumer):
    def process(self, item: Item) -> object:
        time.sleep(.3)
        return item.value * 3


class PipelineTestCase(unittest.TestCase):
    def test_raw_pipeline(self):
        output_queue: Queue[Item] = Queue()

        # 创建节点
        plus_node = Node.create(Plus, 2)
        multiply_node = Node.create(Multiply, 1)

        plus_node.to(multiply_node)
        multiply_node.to(output_queue)

        progress_center = ProgressCenter([plus_node, multiply_node], total=16)

        plus_node.start()
        multiply_node.start()
        progress_center.start()

        for i in range(16):
            plus_node.input_queue.put(Item(i))

        plus_node.stop()
        multiply_node.stop()
        progress_center.stop()

        while not output_queue.empty():
            print(output_queue.get().value)

        self.assertTrue(True)

    def test_func_pipeline(self):
        pipeline = Pipeline([(multiply, 1), (plus, 1)], 16)

        pipeline.start()

        for i in range(16):
            pipeline(i)

        pipeline.stop()

        while not pipeline.empty():
            print(pipeline.get().value)

        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
