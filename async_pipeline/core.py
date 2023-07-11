from multiprocessing import Process, Queue
from copy import deepcopy
from threading import Thread
import inspect
from typing import List, Any, Dict, Callable, Union, Tuple

from tqdm import tqdm

CONFIG_TEMPLATE = {
    "n": 1,
    "worker": {
        "class": None,
        "func": None
    }
}


class End:
    pass


class Item(object):
    def __init__(self, value: Any, channel: str = 'default', batch_size: int = 1):
        self.value: Any = value
        self.channel: str = channel
        self.batch_size: int = batch_size


class NodeBase(object):
    def __init__(self, input_queue: Queue = None,
                 output_queue_dict: Dict[str, Queue] = None, channel: str = None, input_queue_max_size: int = 0):
        super().__init__()
        self.input_queue: Queue = input_queue or Queue(maxsize=input_queue_max_size)
        self.output_queue_dict: Dict[str, Queue] = output_queue_dict or {}
        self.channel: str = channel or self.__class__.__name__

    def to(self, node: Union["NodeBase", Queue], replace: bool = False):
        channel = node.channel if hasattr(node, 'channel') else 'default'
        if channel in self.output_queue_dict and not replace:
            raise AssertionError('Duplicated output channel')
        self.output_queue_dict[channel] = node.input_queue if hasattr(node, 'input_queue') else node


class MixIn(NodeBase):
    def __init__(self, input_queue: Queue = None, output_queue_dict: Dict[str, Queue] = None,
                 wait_seconds: float = 0.5, thread_id: int = -1, *args, **kwargs):
        super().__init__(input_queue, output_queue_dict, *args, **kwargs)
        self.wait_seconds: float = wait_seconds
        self.thread_id: int = thread_id

    def process(self, item: Item) -> object:
        """
        :param item: input from self.queue_in
        :return: None
        """
        raise NotImplementedError

    def stop(self: Union[Thread, Process, "MixIn"]) -> None:
        self.input_queue.put(End)
        self.join()

    def output(self, value: Any, batch_size: int) -> None:
        for queue in self.output_queue_dict.values():
            queue.put(Item(value, self.channel, batch_size))

    def run(self) -> None:
        for item in iter(self.input_queue.get, End):
            result = self.process(item)
            if result is not None:  # 返回 None 代表不需要入队
                self.output(result, item.batch_size)


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
    def __init__(self, node_list: List[NodeBase], total: int = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bar_dict: Dict[str, tqdm] = {}
        self.node_channel_list: List[str] = []

        for i, node in enumerate(node_list):
            node.to(self)
            bar = tqdm(total=total, desc=node.channel, position=i)
            self.bar_dict[node.channel] = bar
            self.node_channel_list.append(node.channel)

    def __getitem__(self, idx: Union[str, int]) -> tqdm:
        if isinstance(idx, str):
            return self.bar_dict[idx]
        elif isinstance(idx, int):
            return self.bar_dict[self.node_channel_list[idx]]
        raise AssertionError(f'type of idx {type(idx)} not belongs to [str, int]')

    def process(self, item: Item):
        self[item.channel].update(item.batch_size)


def coalesce(*args) -> str:
    for each in args:
        if each is not None:
            return each.__name__


class Node(NodeBase):
    def __init__(self, consumer_list: List[Union[ProcessConsumer, ThreadConsumer]], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consumer_list: List[Union[ProcessConsumer, ThreadConsumer]] = consumer_list

        for consumer in self.consumer_list:
            consumer.input_queue = self.input_queue
            consumer.output_queue_dict = self.output_queue_dict
            consumer.channel = self.channel

    @classmethod
    def create(cls, factory: Callable[[Any], Union[ProcessConsumer, ThreadConsumer]], n: int, channel: str = None,
               *args, **kwargs):
        return cls([factory(*args, **{**kwargs, 'thread_id': i}) for i in range(n)],
                   channel=channel or factory.__name__)

    @classmethod
    def create_with_function(
            cls, func: Callable[[Item], Any], factory: Callable[[Any], Union[ProcessConsumer, ThreadConsumer]],
            n: int, channel: str = None, *args, **kwargs):
        instance_list = [factory(*args, **{**kwargs, 'thread_id': i}) for i in range(n)]
        for instance in instance_list:
            instance.process = func
        return cls(instance_list, channel=channel or func.__name__)

    @classmethod
    def from_config(cls, config: dict) -> "Node":
        config = deepcopy(config)
        n = config.pop('n')
        worker_config = config.pop('worker', {})
        factory = config.pop('class', worker_config.pop('class', None))
        func = config.pop('func', worker_config.pop('func', None))

        if factory is None:
            if func is not None:
                factory = ProcessConsumer
            else:
                raise AssertionError("class and func can't be None at the same time")
        config.setdefault('channel', coalesce(func, factory))

        instance_list = [factory(**{**worker_config, 'thread_id': i}) for i in range(n)]
        if func is not None:
            for instance in instance_list:
                instance.process = func
        return cls(instance_list, **config)

    def start(self):
        for consumer in self.consumer_list:
            consumer.start()

    def stop(self):
        for consumer in self.consumer_list:
            consumer.input_queue.put(End)
        for consumer in self.consumer_list:
            consumer.join()


class Pipeline:
    def __init__(self, node_list: List[Node], total: int = None):
        self.node_list: List[Node] = node_list
        self.total: int = total
        self.input_queue = self.node_list[0].input_queue
        self.output_queue = Queue()

        for i in range(len(self.node_list)):
            if i + 1 < len(self.node_list):
                self.node_list[i].to(self.node_list[i + 1])
            else:
                self.node_list[i].to(self.output_queue)

        self.progress_center = ProgressCenter(self.node_list, total=self.total)

    @classmethod
    def from_config(cls, config_list: List[Dict[str, Any]], total: int = None) -> "Pipeline":
        node_list: List[Node] = [Node.from_config(config) for config in config_list]
        return cls(node_list, total)

    @classmethod
    def from_legacy(
            cls,
            factory_list: List[Tuple[Union[Callable[[Item], Any], Callable[[Any], NodeBase]], int, Dict[str, Any]]],
            total: int = None
    ) -> "Pipeline":
        node_list = []
        for factory, n, kwargs in factory_list:
            if inspect.isclass(factory):
                node_list.append(Node.create(factory, n, **kwargs))
            elif inspect.isfunction(factory):
                node_list.append(Node.create_with_function(factory, ProcessConsumer, n, **kwargs))
        return cls(node_list, total)

    def __call__(self, value: Any, batch_size: int = 1) -> None:
        self.input_queue.put(Item(value, 'pipeline_input', batch_size=batch_size))

    def get(self) -> Item:
        return self.output_queue.get()

    def empty(self) -> bool:
        return self.output_queue.empty()

    def start(self):
        self.progress_center.start()
        for node in self.node_list:
            node.start()

    def stop(self):
        import time
        bar: tqdm = self.progress_center.bar_dict[self.node_list[-1].channel]
        if bar.total is not None:
            while bar.n < bar.total:
                time.sleep(1)
                continue
        for node in self.node_list:
            node.stop()
        self.progress_center.stop()
