from multiprocessing import Process, Queue
from threading import Thread
from typing import List, Any, Dict, Callable, Union

from tqdm import tqdm


class End:
    pass


class Item(object):
    def __init__(self, value: Any, channel: str = 'default'):
        self.channel = channel
        self.value = value


class NodeBase(object):
    def __init__(self, input_queue: Queue = None, output_queue_dict: Dict[str, Queue] = None, channel: str = None):
        super().__init__()
        self.input_queue: Queue = input_queue or Queue()
        self.output_queue_dict: Dict[str, Queue] = output_queue_dict or {}
        self.channel: str = channel or self.__class__.__name__

    def to(self, node: Union["NodeBase", Queue], replace: bool = False):
        channel = node.channel if hasattr(node, 'channel') else 'default'
        if channel in self.output_queue_dict and not replace:
            raise AssertionError('Duplicated output channel')
        self.output_queue_dict[channel] = node.input_queue if hasattr(node, 'input_queue') else node


class MixIn(NodeBase):
    def __init__(self, input_queue: Queue = None, output_queue_dict: Dict[str, Queue] = None,
                 wait_seconds: float = 0.5, *args, **kwargs):
        super().__init__(input_queue, output_queue_dict, *args, **kwargs)
        self.wait_seconds: float = wait_seconds

    def process(self, item: Item) -> object:
        """
        :param item: input from self.queue_in
        :return: None
        """
        raise NotImplementedError

    def stop(self: Union[Thread, Process, "MixIn"]) -> None:
        self.input_queue.put(End)
        self.join()

    def run(self) -> None:
        for item in iter(self.input_queue.get, End):
            output = self.process(item)
            if output is not None:  # 返回 None 代表不需要入队
                for queue in self.output_queue_dict.values():
                    queue.put(Item(output, self.channel))


class ThreadConsumer(MixIn, Thread):
    """
    这样继承的话，先执行 MixIn 的 init，后执行 Thread 的 init
    MixIn 用剩下的 *args 和 **kwargs 会给 Thread
    MixIn 的 run 也会覆写 Thread 的 run
    """
    pass


class ProcessConsumer(MixIn, Process):
    pass


class ProgressCenter(ThreadConsumer):
    def __init__(self, node_list: List[NodeBase], total: int = None, batch: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch: bool = batch
        self.bar_dict: Dict[str, tqdm] = {}

        for i, node in enumerate(node_list):
            node.to(self)
            bar = tqdm(total=total, desc=node.channel, position=i)
            self.bar_dict[node.channel] = bar

    def __getitem__(self, channel: str) -> tqdm:
        return self.bar_dict[channel]

    def process(self, item: Item):
        self[item.channel].update(len(item.value) if self.batch else 1)


class Node(NodeBase):
    def __init__(self, consumer_list: List[Union[ProcessConsumer, ThreadConsumer]], channel: str):
        super().__init__(channel=channel)
        self.consumer_list: List[Union[ProcessConsumer, ThreadConsumer]] = consumer_list

        for consumer in self.consumer_list:
            consumer.input_queue = self.input_queue
            consumer.output_queue_dict = self.output_queue_dict
            consumer.channel = self.channel

    @classmethod
    def create(cls, factory: Callable[[Any], Union[ProcessConsumer, ThreadConsumer]], n: int, channel: str = None,
               *args, **kwargs):
        return cls([factory(*args, **kwargs) for _ in range(n)], channel=channel or factory.__name__)

    def start(self):
        for consumer in self.consumer_list:
            consumer.start()

    def stop(self):
        for consumer in self.consumer_list:
            consumer.input_queue.put(End)
        for consumer in self.consumer_list:
            consumer.join()
