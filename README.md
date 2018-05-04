# AMBARI 源码分析之底层状态机实现

##  一、AMBARI 简介

        ambari 是 hadoop 分布式集群配置管理工具，是由hortonworks主导的开源项目。它已经成为apache基金会的孵化器项目，已经成为hadoop运维系统中的得力助手，有关其架构讲解等请关注鄙人后续公众号更新。

##  二、关于状态机

        状态机是有一组状态组成，这些状态分为三类，分别为初始状态、中间状态、最终状态，状态机首先由初始状态开始运行，经过一系列的中间状态达到最终状态，并在最终状态退出，从而形成一个有向无环图，其状态处理逻辑是收到一个事件，触发状态preState到postState的转换，转换操作是由事件对应的hook完成的。    

        AMBARI引入了事件（消息）发布订阅框架eventBus和状态机机制，eventBus在ambari中使用在后续文章为大家介绍，这里主要从源码的角度解析ambari状态机机制中的关键类StateMachineFactory，以host的状态转换为例。

##  三、 HostImpl 内部状态

        AMBARI 中的host、serviceComponentHost 等有其自身所处一系列的状态，例如HostImpl内部的一系列状态都定义在HostState中，

```java
public enum HostState {
  /**
   * New host state
   */
  INIT,
  /**
   * State when a registration request is received from the Host but
   * the host has not responded to its status update check.
   */
  WAITING_FOR_HOST_STATUS_UPDATES,
  /**
   * State when the server is receiving heartbeats regularly from the Host
   * and the state of the Host is healthy
   */
  HEALTHY,
  /**
   * State when the server has not received a heartbeat from the Host in the
   * configured heartbeat expiry window.
   */
  HEARTBEAT_LOST,
  /**
   * Host is in unhealthy state as reported either by the Host itself or via
   * any other additional means ( monitoring layer )
   */
  UNHEALTHY;
}
```

        HostImpl 的内部状态包括新建（INIT）、接受注册（ WAITING\_FOR\_HOST\_STATUS\_UPDATES）、健康（ HEALTHY）、心跳丢失（ HEARTBEAT\_LOST）等 。

## 四、 HostImpl 中的 StateMachineFactory

        ambari主机的状态转换在 HostImpl中声明， 首先声明了一个静态final类型的属性stateMachineFactory，代码如下：

```text
  private static final StateMachineFactory
    <HostImpl, HostState, HostEventType, HostEvent>
      stateMachineFactory
        = new StateMachineFactory<HostImpl, HostState, HostEventType, HostEvent>
        (HostState.INIT)

   // define the state machine of a Host

   // Transition from INIT state
   // when the initial registration request is received
   .addTransition(HostState.INIT, HostState.WAITING_FOR_HOST_STATUS_UPDATES,
       HostEventType.HOST_REGISTRATION_REQUEST, new HostRegistrationReceived())
   // when a heartbeat is lost right after registration
   .addTransition(HostState.INIT, HostState.HEARTBEAT_LOST,
       HostEventType.HOST_HEARTBEAT_LOST, new HostHeartbeatLostTransition())
              
   ....// 若干状态转换的定义

   .installTopology();
```

        StateMachineFactory  负责产生状态机, 则HostImpl中必有一个属性来接收该工厂产生的状态机类，代码如下：

```text
  public HostImpl(@Assisted HostEntity hostEntity, Gson gson, HostDAO hostDAO, HostStateDAO hostStateDAO) {
    ...
    // 从stateMachineFactory中得到一个stateMachine对象
    stateMachine = stateMachineFactory.make(this);
    ...

```

       下面从状态机的调用流程中解析下StateMachineFactory中的属性与方法，首先查看该类结构与 StateMachineFactory类的声明

![](.gitbook/assets/image%20%281%29.png)

```text
/**
 * State machine topology.
 * This object is semantically immutable.  If you have a
 * StateMachineFactory there's no operation in the API that changes
 * its semantic properties.
 *
 * @param <OPERAND> The object type on which this state machine operates.
 * @param <STATE> The state of the entity.
 * @param <EVENTTYPE> The external eventType to be handled.
 * @param <EVENT> The event object.
 *
 */
final public class StateMachineFactory
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
              ....
```

       从类的注释中可以知道StateMachineFactory是一组状态的拓扑图，OPERAND是这组状态机的操作者，STATE是这组状态机中的状态，EVENTTYPE是触发状态转移的事件类型，EVENT是触发状态转移的事件，它包含的四个属性信息：

1.  defaultInitialState：对象创建时，内部有限状态机的默认初始状态,比如：HostImpl的内部状态机默认初始状态是HostState.INIT
2.  optimized：布尔类型，用于标记当前状态机是否需要优化性能，即构建状态拓扑表stateMachineTable
3.  stateMachineTable：状态拓扑表，在optimized为真时，通过对transitionsListNode链表进行处理产生
4.  transitionsListNode：过渡列表节点，将状态机的一个个过渡的ApplicableTransition实现串联为一个列表，每个节点包含一个ApplicableTransition实现及指向下一个节点的引用，其实现见代码

```text

  private class TransitionsListNode {
    final ApplicableTransition transition;
    final TransitionsListNode next;

    TransitionsListNode
        (ApplicableTransition transition, TransitionsListNode next) {
      this.transition = transition;
      this.next = next;
    }
  }
```

        有两个属性，分别为ApplicableTransition\(一个接口，ApplicableSingleOrMultipleTransition实现了该接口\)的transition和TransitionsListNode的next属性。从构造函数中可以看出transition是当前状态转移对应的处理类，next指向的是下一个TransitionsListNode。

        StateMachineFactory的状态拓扑图是通过多种addTransition让用户添加各种状态转移，最后通过installTopology完成一个状态机拓扑的搭建，其中初始状态是通过StateMachineFactory的构造函数指定的

```text
  public StateMachineFactory(STATE defaultInitialState) {
    this.transitionsListNode = null;
    this.defaultInitialState = defaultInitialState;
    this.optimized = false;
     // 用于存放preState、eventType和transition的映射关系
    this.stateMachineTable = null;
  }
```

          HostImpl 中的初始状态是INIT, 由其初始状态初始化一个StateMachineFactory实例，然后通过addTransition注册各种状态转移。

####  addTransition

         我们接着看 StateMachineFactory中包含的5个addTransition方法，可以看出其定义了三种状态转换方式，如下所示：

1. preState通过某个事件转换为postState，也就是状态机在preState时，接收到Event事件后，执行对应的hook，并在执行完成后将当前的状态转换为postState.

```text
addTransition(STATE preState, STATE postState,
                        EVENTTYPE eventType,
                        SingleArcTransition<OPERAND, EVENT> hook)
```

    2. preState通过多个事件转换为postState，也就是状态机在preState时，接收到某些Event事件后，执行对应的hook，并在执行完成后将当前的状态转换为postState.

```text
addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes,
      SingleArcTransition<OPERAND, EVENT> hook)
```

    3. preState通过某个事件转换为多个postState，也就是状态机在preState时，接收到Event事件后，执行对应的hook，并在执行完成后将返回hook的返回值所表示的状态.

```text
addTransition(STATE preState, Set<STATE> postStates,
                        EVENTTYPE eventType,
                        MultipleArcTransition<OPERAND, EVENT, STATE> hook)
```

     这里挑选 1中的addTransition解析，代码如下：

```text
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState,
                        EVENTTYPE eventType,
                        SingleArcTransition<OPERAND, EVENT> hook){
    return new StateMachineFactory
        (this, new ApplicableSingleOrMultipleTransition
           (preState, eventType, new SingleInternalArc(postState, hook)));
  }
```

      可以看出在addTransition中又new了一个新的StateMachineFactory。这里涉及到两个类ApplicableSingleOrMultipleTransition和SingleInternalArc.

     我们先看 SingleInternalArc 类，可以看到它对 SingleArcTransition做了封装，

```text
public interface SingleArcTransition<OPERAND, EVENT> {
  /**
   * Transition hook.
   *
   * @param operand the entity attached to the FSM, whose internal
   *                state may change.
   * @param event causal event
   */
  public void transition(OPERAND operand, EVENT event);

}

```

       因为 SingleArcTransition的具体实现类只负责接收到事件后的具体操作或行为， 并没有包含状态相关的信息，所以在状态机执行状态转换时，并不是直接调用SingleArcTransition具体实现类的transition方法， 而是 SingleInternalArc作为Transition接口的实现类，在代理SingleArcTransition的同时，负责状态变换，代码如下.

```text

  private class SingleInternalArc
                    implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {

    private STATE postState;
    private SingleArcTransition<OPERAND, EVENT> hook; // transition hook

    SingleInternalArc(STATE postState,
        SingleArcTransition<OPERAND, EVENT> hook) {
      this.postState = postState;
      this.hook = hook;
    }
    
    @Override
    public STATE doTransition(OPERAND operand, STATE oldState,
                              EVENT event, EVENTTYPE eventType) {
      if (hook != null) {
        hook.transition(operand, event);
      }
      return postState;
    }
  }
```

        同理对应的处理多个状态MultipleInternalArc类，其逻辑结构和SingleInternalArc类似，只是在hook处理结束之后，判断下postState是否在备选的状态集中，代码如下： 

```text
  private class MultipleInternalArc
              implements Transition<OPERAND, STATE, EVENTTYPE, EVENT>{

    // Fields
    // 可选的postState状态
    private Set<STATE> validPostStates;
    private MultipleArcTransition<OPERAND, EVENT, STATE> hook;  // transition hook

    MultipleInternalArc(Set<STATE> postStates,
                   MultipleArcTransition<OPERAND, EVENT, STATE> hook) {
      this.validPostStates = postStates;
      this.hook = hook;
    }

    @Override
    public STATE doTransition(OPERAND operand, STATE oldState,
                              EVENT event, EVENTTYPE eventType)
        throws InvalidStateTransitionException {
      STATE postState = hook.transition(operand, event);

      if (!validPostStates.contains(postState)) {
        throw new InvalidStateTransitionException(oldState, eventType);
      }
      return postState;
    }
  }
```

      接下来看下ApplicableSingleOrMultipleTransition， 为了将所有状态机中的状态过渡与状态建立起映射关系， StateMachineFactory中提供了ApplicableTransition接口用于将SingleInternalArc和MultipleInternalArc添加到状态机的拓扑表中，ApplicableTransition的实现类为ApplicableSingleOrMultipleTransition类，其apply方法用于代理SingleInternalArc和MultipleInternalArc，将它们添加到状态拓扑表中，ApplicableTransition，ApplicableSingleOrMultipleTransition的代码如下：

```text

  private interface ApplicableTransition
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
  }
```

```text
  static private class ApplicableSingleOrMultipleTransition
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT>
          implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {
    final STATE preState;
    final EVENTTYPE eventType;
    final Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

    ApplicableSingleOrMultipleTransition
        (STATE preState, EVENTTYPE eventType,
         Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
      this.preState = preState;
      this.eventType = eventType;
      this.transition = transition;
    }

    @Override
    public void apply
             (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
        = subject.stateMachineTable.get(preState);
      if (transitionMap == null) {
        // I use HashMap here because I would expect most EVENTTYPE's to not
        //  apply out of a particular state, so FSM sizes would be
        //  quadratic if I use EnumMap's here as I do at the top level.
        transitionMap = new HashMap<EVENTTYPE,
          Transition<OPERAND, STATE, EVENTTYPE, EVENT>>();
        subject.stateMachineTable.put(preState, transitionMap);
      }
      transitionMap.put(eventType, transition);
    }
  }
```

#### installTopology

       addTransition把状态都添加到StateMachineFactory中之后，通过调用installTopology进行状态链的初始化

```text
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          installTopology() {
    return new StateMachineFactory(this, true);
  }

```

        这里实例化了一个新的StateMachineFactory，不同的是将属性optimized设置为true.将调用如下构造方法

```text
  private StateMachineFactory
      (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
       boolean optimized) {
    this.defaultInitialState = that.defaultInitialState;
    this.transitionsListNode = that.transitionsListNode;
    this.optimized = optimized;
    if (optimized) {
      makeStateMachineTable();
    } else {
      stateMachineTable = null;
    }
  }
```

       当optimized为true是，会在构造函数中调用makeStateMachineTable对stateMachineTable进行赋值.其代码清单如下：

```text
  private void makeStateMachineTable() {
  
    //创建堆栈stack，用于将transitionsListNode链表中各个节点持有的ApplicableSingleOrMultipleTransition压入栈中；
    Stack<ApplicableTransition> stack = new Stack<ApplicableTransition>();

    //创建状态拓扑表stateMachineTable，
    //并在此拓扑表中插入一个额外的默认初始状态defaultInitialState与null的映射；
    Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>
      prototype = new HashMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();

    prototype.put(defaultInitialState, null);

    // I use EnumMap here because it'll be faster and denser.  I would
    //  expect most of the states to have at least one transition.
    stateMachineTable
       = new EnumMap<STATE, Map<EVENTTYPE,
                           Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);
    //迭代访问transitionsListNode链表，并将各个节点持有的ApplicableSingleOrMultipleTransition压入栈中；
    for (TransitionsListNode cursor = transitionsListNode;
         cursor != null;
         cursor = cursor.next) {
      stack.push(cursor.transition);
    }
    //依次弹出栈顶的ApplicableSingleOrMultipleTransition，并应用其apply方法，持续不断的构建状态拓扑表stateMachineTable。
    while (!stack.isEmpty()) {
      stack.pop().apply(this);
    }
  }
```

       下面看下ApplicableSingleOrMultipleTransition.apply方法

```text
    @Override
    public void apply
             (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
      // 从stateMachineTable中拿到preState对应的transitionMap
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
        = subject.stateMachineTable.get(preState);
      // transitionMap为null则new一个transitionMap的HashMap
      if (transitionMap == null) {
        // I use HashMap here because I would expect most EVENTTYPE's to not
        //  apply out of a particular state, so FSM sizes would be
        //  quadratic if I use EnumMap's here as I do at the top level.
        transitionMap = new HashMap<EVENTTYPE,
          Transition<OPERAND, STATE, EVENTTYPE, EVENT>>();
        subject.stateMachineTable.put(preState, transitionMap);
      }
       // 将eventType对应的Transition放入transitionMap中
      transitionMap.put(eventType, transition);
    }
```

## 五、触发状态转移

         HostImpl 中状态转移的触发是在HostImpl.handleEvent 中触发的, 在 handleEvent调用了stateMachine.doTransition\(event.getType\(\), event\); 而 stateMachine是在 HostImpl的构造方法中调用`stateMachineFactory.make(this)`进行实例化的。

```text
  public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
    return new InternalStateMachine(operand, defaultInitialState);
  }

```

          在make中new了一个InternalStateMachine对象，则stateMachine其实是InternalStateMachine类的对象，stateMachine.doTransition的调用其实就是stateMachine.doTransition的doTransition方法，然而在stateMachine.doTransition方法中又调用了StateMachineFactory的doTransition方法，StateMachineFactory.doTransition代码如下：

```text
  /**
   * Effect a transition due to the effecting stimulus.
   * @param state current state
   * @param eventType trigger to initiate the transition
   * @param cause causal eventType context
   * @return transitioned state
   */
  private STATE doTransition
           (OPERAND operand, STATE oldState, EVENTTYPE eventType, EVENT event)
      throws InvalidStateTransitionException {
    // We can assume that stateMachineTable is non-null because we call
    //  maybeMakeStateMachineTable() when we build an InnerStateMachine ,
    //  and this code only gets called from inside a working InnerStateMachine .
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
      = stateMachineTable.get(oldState);
    if (transitionMap != null) {
      Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition
          = transitionMap.get(eventType);
      if (transition != null) {
        return transition.doTransition(operand, oldState, event, eventType);
      }
    }
    throw new InvalidStateTransitionException(oldState, eventType);
  }
```

         doTransition方法的执行步骤如下：

1. 根据组件（例如HostImpl）当前状态（oldState）从状态拓扑表stateMachineTable中获取oldState对应的Transition映射表；
2. 如果oldState对应的Transition映射表不为null，则根据事件类型EVENTTYPE从映射表中获取对应的Transition；
3.  如果存在对应的Transition，那么调用其doTransition方法进行真正的转态转移；

        至此一次状态转移就完成了。

## 六、状态转移代码示例

        下面以HostEventType.HOST\_REGISTRATION\_REQUEST事件类型被触发之后，发送的状态转移。

* 首先 该事件在HostImpl. handleEvent中被处理，调用 stateMachine.doTransition\(event.getType\(\), event\)方法
* 跟踪到InternalStateMachine.doTransition, 在其内又继续调用StateMachineFactory.this.doTransition
* 然后根据HostEventType.HOST\_REGISTRATION\_REQUEST事件被触发时的状态HostState.INIT，从stateMachineTable中EventType和Transition的映射关系transitionMap 拿到 HostEventType.HOST\_REGISTRATION\_REQUEST对应的transition\(SingleInternalArc\) 即new      HostRegistrationReceived\(\)
*  接着调用transition的doTransition方法，这里才调用到该状态转移涉及到的hook，也就是在addTransition中注册的hook类 HostRegistrationReceived

        代码流程为：

```text
 stateMachine[InternalStateMachine].doTransition -> 
 StateMachineFactory.this.doTransition -> transition[SingleInternalArc].doTransition 
 -> hook[HostRegistrationReceived].transition
```

此流程结束之后HOST的状态由HostState.INIT转为HostState.WAITING\_FOR\_HOST\_STATUS\_UPDATES，这套流程是在addTransition\(HostState.INIT,HostState.WAITING\_FOR\_HOST\_STATUS\_UPDATES,HostEventType.HOST\_REGISTRATION\_REQUEST, new HostRegistrationReceived\(\)\)中规定的。

其实如果看过yarn中的状态机实现，不难发现他们逻辑是一样的，今天天色也晚，改天将分享ambari的整体架构和ambari中事件（消息）发布与订阅框架eventBus的使用，

感谢关注。 

