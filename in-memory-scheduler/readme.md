<!-- ИЗМЕНЕНО В ЗАДАЧЕ 28.1: описание публичного JSON-контракта. -->
## HTTP DTO

JSON-контракт определён в `internal/httpdto`; HTTP-обработчики явно преобразуют
запросы в `domain.Job`, а модели задач и статистику — в DTO ответов.
`domain.Job` не сериализуется в HTTP напрямую. Внутреннее поле `SomeSlice`
не принимается в запросе и не возвращается в ответе; `id` и `state` задаёт сервер.
Неизвестные поля запроса отклоняются с HTTP 400.

- `POST /jobs`: `name`, `description`, `complexity`, `attrs`, `tags`.
  `complexity` имеет тип `*int`: отсутствие или `null` дают HTTP 422,
  явный `0` передаётся планировщику как значение. Проверка диапазона 1–10
  остаётся обязанностью доменного валидатора.
  `name` и `description` имеют тип `string`: отсутствие и пустая строка
  равнозначны при создании. Для них `*string` не нужен.
  Ответ: `{"name":"example"}`; `Scheduler.Add` пока не возвращает ID.
- Ответ задачи: `id`, `name`, `description`, `complexity`, `attrs`, `tags`,
  `state`. `attrs` и `tags` — JSON-объекты, пустые коллекции возвращаются как `{}`.
  `state` — стабильная строка: `"pending"`, `"running"`, `"failed"`,
  `"done"`, `"cancelled"` или `"stopped"`.
- Ошибки обработчиков и middleware: `{"error":"message"}`.
- `GET /stats`: `{"done":0,"failed":0,"cancelled":0}`.
  Поля ответов — значения без `omitempty`: нули и пустые строки не исчезают.

### JSON числа и динамические значения

Когда `encoding/json` декодирует JSON-число в `any`, стандартный тип значения —
`float64`. Целые числа больше `2^53` могут округлиться. `Decoder.UseNumber()`
сохраняет их как `json.Number`, откуда можно получить исходную десятичную строку
или разобрать число в `int64`/`float64` явно. Для известных схем API используются
типизированные DTO, поэтому бизнес-логика не зависит от `map[string]any`.

### Потоковый экспорт jobs

`GET /jobs/export` возвращает NDJSON (`application/x-ndjson`): каждая строка —
отдельный `JobResponse`. NDJSON позволяет клиенту декодировать записи по одной,
не накапливая весь экспорт в памяти; сервер сбрасывает каждую запись в поток.
Порядок записей стабилен: по возрастанию ID. `httpclient.Client.ExportJobs`
передаёт каждую декодированную запись callback-функции и прекращает чтение при
отмене переданного context.

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
