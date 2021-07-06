# 2021_SeoKangHwi
- 소속: Global Platform Dev
- 멘토: 이양일(yangil.lee@worksmobile.com)

## 인턴쉽 과제(Mini Kafka 개발)
### 필수 스펙
- Producer
  + Topic 을 지정하여 message publish
  + multiple producer
  + 메세지 publish 순서 보장. 예외가 발생하는 케이스를 제외하고 같은 topic 에 publish 할 경우 먼저 publish API 를 호출한 메세지가 먼저 broker 에 전달되야 한다.
- Consumer
  + 지정한 Topic 의 message consume
  + Multiple Consumer 운영 가능
  + Consumer Group 기능(자세한 설명은 [link](https://www.popit.kr/kafka-consumer-group/) 참고)
  + Partition Ownership(같은 Consumer Group 일 경우 Partition 을 점유한 consumer 만 message 를 consume 할 수 있다.)
- Broker 
  + Message 수신 및 저장. Consume 기능
  + Consumer Group 및 offset 등의 metadata 관리(zookeeper 혹은 그외의 다른 솔루션 사용 가능)  

### 선택적 스펙
+ multiple partitions
+ Replica 및 Broker Fail Over.
+ zookeeper 가 아닌 다른 분산 코디네이터 사용 혹은 직접 구현.
+ 기타 kafka 이슈 개선.

## 일정 및 계획
### 1주차(07.05 ~ 07.09)
- Kafka 학습
- 개발 계획 및 설계

### 2주차(07.12 ~ 07.16)
- 개발 계획 및 설계
- 개발 진행

### 3주차(07.19 ~ 07.23)
- 개발 진행

### 4주차(07.26 ~ 07.30)
- 개발 진행
- 중간발표

### 5주차(08.02 ~ 08.06)
- 개발 진행

### 6주차(08.09 ~ 08.13)
- 개발 진행

### 7주차(08.16 ~ 08.20)
- 개발 진행

### 8주차(08.23 ~ 08.27)
- 개발 진행
- 최종 발표
