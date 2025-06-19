# Blockhouse Quant Developer Work Trial

## Deliverables
- `kafka_producer.py`: Streams l1_day.csv to Kafka
- `backtest.py`: Consumes stream, applies allocator logic
- `README.md`: Approach, tuning logic, EC2 setup
- `results.png/pdf` (optional): Plot of cumulative execution cost

## Setup Instructions

### Requirements
- Python 3.8+
- pandas, numpy
- kafka-python
- Kafka & Zookeeper (local or EC2)

### Installation
```bash
pip install pandas numpy kafka-python
```

### Running Kafka Producer
```bash
python kafka_producer.py
```

### Running Backtest
```bash
python backtest.py
```

## EC2 Deployment

*Instructions for EC2 setup will go here.* 
