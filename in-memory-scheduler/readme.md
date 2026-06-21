Ownership of chans

type Validator struct {
    request chan ValidationRequest
    jobs    chan Job
    stop    chan struct{}
}


results
получает его снаружи
пишет: Worker
читает: Validator
закрывает: Worker

stop
создается в этой структуре
пишет никто
читают Validator
закрывает Validator

validationRequest
сам создает  App
пишет: Scheduler
читает: Validator
закрывает: никто не закрывает

пишет в reply chan ValidationResult когда вытаскивает его из request. 
то есть получается его не явно и закрывать его до валидации нельзя, так как он writer 


Middleware

ProcessFunc(func(job Job, cancel <-chan struct{})  - снаружи cancel передается дальше в процессоры больше ничего

type PrintProcessor struct{}

все процессоры получают cancel как параметр и делают resturn о есть только его читают. 
канал получается должен быть закрыт сверху. и он закрывается из Worker.Loop при stop

type Worker struct {
    stop              chan struct{}
    jobs              chan Job
    results           chan<- Results
    heartBeat         chan<- HeartBeat
}

stop
сам создает Worker
пишет: никто не пишет
читает: Worker
закрывает: App


jobs
сам создает Worker
пишет: пишет Worker
читает: Worker
закрывает: App


results
создает App
пишет:  Worker
Process возвращает Result в resultChan, потом из resultChan передается в w.results
читает: Scheduler
закрывает: нельзя закрывать

heartBeat
создает App
пишет:  Worker
читает: Scheduler
закрывает: нельзя закрывать


type Scheduler struct {
    stop              chan struct{}
    results           []chan Results
    heartBeat         chan HeartBeat
    validationRequest chan ValidationRequest
    mergedChan        chan Results
}

stop
сам создает Scheduler
пишет: никто не пишет
читает: Scheduler
закрывает: Scheduler


results
сам создает Scheduler
пишет: App аппендит у себя result который воркеры будут возвращать
читает: Scheduler
закрывает: никто не закрывает

heartBeat
сам создает App
пишет: Worker
читает: Scheduler
закрывает: никто не закрывает

validationRequest
сам создает  App
пишет: Scheduler
читает: Validator
закрывает: никто не закрывает


mergedChan
сам создает  Scheduler
пишет: Scheduler
читает: Scheduler
закрывает: никто не закрывает


type App struct {
    heartBeat  chan HeartBeat
    results    []chan Results
}

heartBeat
сам создает App
пишет: передает его шедулеру и воркеру как мараметр
читает: не читает сам
закрывает: не закрывает

results
сам создает App
пишет: передает его шедулеру и воркеру как мараметр
читает: не читает сам
закрывает: не закрывает