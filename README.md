## Mini Kafka
- Netty를 이용하여 mini kafka를 설계하고 구현해본다. Producer, Consumer, Broker에 대한 충분한 이해와 공부를 통해 
각 컴포넌트 스펙을 정의하고 이를 구현 및 테스트를 확인한다 

<br>

## Teck Stack

- Framework - Netty (Implementing native Netty without spring framework)
- Project Management tool : maven 
- Language - Java 11
- Protocol - RPC 

<br>

## Structure

<img width="1109" alt="image" src="https://user-images.githubusercontent.com/45396949/130933495-a493e8d4-c82f-41f5-9d9f-3d689e68088c.png">

<br>
  
## Producer

- **정의**

  - 브로커에 존재하는 topic에 message를 publish한다  

- **특징**

  - Topic을 지정하여 message publish한다
     - 비동기적으로 메세지를 전송한다
     - 배치 전송은 하지 않는다
     - 다수의 토픽에 메세지를 전송할 수 있다
  - Multiple producer

  - 메세지 publish 순서 보장
     - key를 지정하여 원하는 파티션으로 전송이 가능하다
     - 별도로 키를 지정하지 않을 시, UTC time과 round-robin을 사용하여 임의대로 partition에 message를 전송한다

  
## Consumer

- **정의**

  - 브로커에게서 message를 얻어오고 처리한다. 

- **특징**

  - 지정한 topic의 message를 consume한다
     - broker로부터 받은 message를 처리하면 offset을 커밋하도록 한다 
     - consumer는 옵션 값으로 지정한 만큼 message를 polling하도록 한다 
     - 배치 기능을 따로 구현하지 않는다

  - Multiple Consumer
  - Consumer Group
     - 여러 컨슈머 그룹을 갖을 수 있다 
     - Properties로부터 그룹을 지정할 수 있다 
  - Partition Ownership
     - partition을 점유한 컨슈머만 해당 partition의 message를 처리할 수 있다
     - 파티션과 컨슈머는 1대1 관계의 ownership을 갖는다 
  - Consumer의 상태체크를 위한 heartbeat을 전송한다

  

## Broker

- **정의**

  - Producer로부터 생성된 message를 topic의 partition에 저장하고 이를 Consumer에게 제공한다 
- **특징**
  - topic의 생성 여부는 옵션 값으로 결정할 수 있다 
   
  - Broker ID를 필수로 입력받는다 
   
  - record를 파일로 저장하고 관리한다 
 
  - consumerGroup의 offset을 파일로 저장하고 관리한다 
  
  - topic을 파일로 저장하고 관리한다 
  
  - consumer의 heartbeat에 따라서 rebalancing을 진행한다 


- **Metadata**
  - Record, Record offset
  - Consumer_offset
  - Consumer Ownership 
  - Topic list 


