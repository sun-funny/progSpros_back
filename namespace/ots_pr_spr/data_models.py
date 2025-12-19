from typing import List, Tuple, Dict, Any
from dataclasses import dataclass, field

@dataclass
class RelColumn:
    """Класс для хранения пар название-значение"""
    name: str
    value: Any

@dataclass
class Consumer:
    """Класс для хранения данных о потребителе"""
    name: str
    rel_columns: List[RelColumn] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Конвертация в словарь"""
        return {
            "name": self.name,
            "rel_columns": [[col.name, col.value] for col in self.rel_columns]
        }

@dataclass
class Demand:
    """Класс для хранения данных о потребителе"""
    rel_columns: List[RelColumn] = field(default_factory=list)

    def to_dict(self) -> Any:
        """Конвертация в словарь"""
        return [[col.name, col.value] for col in self.rel_columns]

@dataclass
class RegionData:
    """Класс для хранения данных по региону"""
    region_name: str
    # active
    active_consumers: List[Consumer] = field(default_factory=list)
    # promising
    promising_total_consumers: Consumer = None
    expected_consumers: List[Consumer] = field(default_factory=list)
    potential_consumers: List[Consumer] = field(default_factory=list)
    #
    expected_demand: List[Demand] = field(default_factory=list)
    max_demand: List[Demand] = field(default_factory=list)
    #
    detalization_active_consumers: List[Consumer] = field(default_factory=list)
    detalization_expected_consumers: List[Consumer] = field(default_factory=list)
    detalization_potential_consumers: List[Consumer] = field(default_factory=list)

    # consumers
    def add_consumers(self, ver_real: str, name: str, data: Dict[str, Any]):
        if ver_real == "действующие потребители":
            self.add_active_consumers(
                name=name,
                data=data
            )
        elif ver_real == "ожидаемые перспективные потребители":
            self.add_expected_consumers(
                name=name,
                data=data
            )
        elif ver_real == "потенциальные перспективные потребители":
            self.add_potential_consumers(
                name=name,
                data=data
            )

    def add_active_consumers(self, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.active_consumers.append(consumer)

    def add_expected_consumers(self, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.expected_consumers.append(consumer)

    def add_potential_consumers(self, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.potential_consumers.append(consumer)

    # total consumers
    def add_total_consumers(self, ver_real: str, name: str, data: Dict[str, Any]):
        if ver_real == "перспективные потребители":
            self.add_promising_total_consumers(
                name=name,
                data=data
            )

    def add_promising_total_consumers(self, name: str, data: Dict[str, Any]):
        """total в promising_consumers"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.promising_total_consumers = consumer

    # detalization consumers
    def add_detalization_consumers(self, ver_real: str, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        if ver_real == "действующие потребители":
            self.add_detalization_active_consumers(
                name=name,
                data=data
            )
        elif ver_real == "ожидаемые перспективные потребители":
            self.add_detalization_expected_consumers(
                name=name,
                data=data
            )
        elif ver_real == "потенциальные перспективные потребители":
            self.add_detalization_potential_consumers(
                name=name,
                data=data
            )

    def add_detalization_active_consumers(self, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.detalization_active_consumers.append(consumer)

    def add_detalization_expected_consumers(self, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.detalization_expected_consumers.append(consumer)

    def add_detalization_potential_consumers(self, name: str, data: Dict[str, Any]):
        """Добавление потребителя"""
        consumer = Consumer(name=name)
        for col_name, col_value in data.items():
            consumer.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.detalization_potential_consumers.append(consumer)

    # demand
    def add_expected_demand(self, data: Dict[str, Any]):
        """Добавление потребителя"""
        demand = Demand()
        for col_name, col_value in data.items():
            demand.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.expected_demand.append(demand)

    def add_max_demand(self, data: Dict[str, Any]):
        """Добавление потребителя"""
        demand = Demand()
        for col_name, col_value in data.items():
            demand.rel_columns.append(RelColumn(name=col_name, value=col_value))
        self.max_demand.append(demand)

    def to_dict_consumers(self) -> Dict:
        """consumers - Конвертация в словарь"""
        return {
            "region_name": self.region_name,
            "active_consumers": [consumer.to_dict() for consumer in self.active_consumers],
            "promising_consumers": {
                 "expected": [consumer.to_dict() for consumer in self.expected_consumers],
                 "potential": [consumer.to_dict() for consumer in self.potential_consumers],
                 "total": self.promising_total_consumers.to_dict() if self.promising_total_consumers else None
             },
            "expected_demand": {
                "rel_columns": demand.to_dict() for demand in self.expected_demand
            },
            "max_demand": {
                "rel_columns": demand.to_dict() for demand in self.max_demand
            },
            "consumers_detalization": {
                "active_consumers": [consumer.to_dict() for consumer in self.detalization_active_consumers],
                "expected_comsumers": [consumer.to_dict() for consumer in self.detalization_expected_consumers],
                "potential_comsumers": [consumer.to_dict() for consumer in self.detalization_potential_consumers],
            }
        }

    def to_dict_total(self) -> Dict:
        """total - Конвертация в словарь"""
        return {
            "region_name": self.region_name,
            "active_consumers": [consumer.to_dict() for consumer in self.active_consumers],
            "promising_consumers": {
                 "expected": [consumer.to_dict() for consumer in self.expected_consumers],
                 "potential": [consumer.to_dict() for consumer in self.potential_consumers],
                 "total": self.promising_total_consumers.to_dict() if self.promising_total_consumers else None
             },
            "expected_demand": {
                "rel_columns": demand.to_dict() for demand in self.expected_demand
            },
            "max_demand": {
                "rel_columns": demand.to_dict() for demand in self.max_demand
            },
        }

@dataclass
class Consumers:
    consumers: List[RegionData] = field(default_factory=list)
    total: List[RegionData] = field(default_factory=list)

    def add_consumer(self, rd :RegionData):
        self.consumers.append(rd)

    def add_total(self, rd :RegionData):
        self.total.append(rd)

    def to_dict(self) -> Dict:
        """Конвертация в словарь"""
        return {
             "consumers": [consumer.to_dict_consumers() for consumer in self.consumers],
             "total": [total.to_dict_total() for total in self.total],
        }