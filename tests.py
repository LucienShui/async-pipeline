import os.path
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
        return item.value + 1


class Multiply(ProcessConsumer):
    def __init__(self, factor: int = 3, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.factor = factor

    def process(self, item: Item) -> object:
        time.sleep(.3)
        return item.value * self.factor


class Writer(ProcessConsumer):

    def __init__(self, filepath: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath: str = filepath

    def process(self, item: Item) -> bool:
        with open(self.filepath, 'a') as f:
            f.write(item.value + '\n')
        time.sleep(1)
        return True


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
        pipeline = Pipeline.from_legacy([(multiply, 1, {}), (plus, 1, {})], 16)

        pipeline.start()

        for i in range(16):
            pipeline(i)

        pipeline.stop()

        while not pipeline.empty():
            print(pipeline.get().value)

        self.assertTrue(True)

    def test_class_pipeline(self):
        pipeline = Pipeline.from_legacy([(Multiply, 2, {}), (Plus, 1, {})], 16)

        pipeline.start()

        for i in range(16):
            pipeline(i)

        pipeline.stop()

        while not pipeline.empty():
            print(pipeline.get().value)

        self.assertTrue(True)

    def test_from_config(self):
        total = 16
        pipeline = Pipeline.from_config([
            {"n": 1, "class": Plus},
            {"func": multiply},
            {"n": 1, "worker": {"func": plus}},
            {"worker": {"class": Multiply, "factor": 2}}
        ], total)

        pipeline.start()

        for i in range(total):
            pipeline(i)

        pipeline.stop()

        while not pipeline.empty():
            print(pipeline.get().value)

        self.assertTrue(True)

    def test_write(self):
        import tempfile
        total = 32
        with tempfile.NamedTemporaryFile() as tmp:
            pipeline = Pipeline.from_config([
                {"n": 4, "worker": {"class": Writer, "filepath": tmp.name}}
            ], total)

            pipeline.start()

            for i in range(total):
                pipeline(f'{i:02d}')

            pipeline.stop()

            result_list = []
            with open(tmp.name) as f:
                for line in f:
                    result_list.append(line.strip())
            result_list.sort()
            for i in range(total):
                self.assertEqual(f'{i:02d}', result_list[i])


class QueueTimeoutTestCase(unittest.TestCase):
    def test_queue_join(self):
        from multiprocessing import JoinableQueue
        q = JoinableQueue()

        q.put(1)
        self.assertEqual(1, q.get())
        q.task_done()
        q.join()

        q.put(2)
        self.assertEqual(2, q.get())
        q.task_done()
        q.join()

        self.assertTrue(True)

    def test_queue_timeout(self):
        from queue import Empty, Full
        from multiprocessing import Queue
        q = Queue(maxsize=3)
        with self.assertRaises(Empty):
            result = q.get(timeout=.5)
        q.put(1)
        q.put(2)
        q.put(3)
        with self.assertRaises(Full):
            q.put(4, timeout=.5)
        result = q.get()
        self.assertEqual(1, result)
        q.put(4)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
