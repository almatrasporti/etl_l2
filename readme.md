## Microservizio ETL_L2

Il modulo ETL_L2, realizzato in linguaggio Java, si occupa di caricare i dati dal topic _batch_ di Kafka, per 
riscriverli, dopo eventuali elaborazioni, su un altro sistema, nel caso in oggetto, Document store MongoDB.

I dati possono essere scritti con modalità/elaborazioni differenti su collezioni differenti. 

Per tale scopo, il microservizio utilizza la **Consumer API** di Kafka e l'**API di comunicazione MongoDB**.

### Configurazione
E' possibile configurare l'ETL_L2 mediante un file di properties, passato contestualmente al lancio del servizio, 
mediante l'opzione java `-Dproperties.file="Injector.properties"`, contenente i seguenti campi:

- **Kafka.servers**: elenco di coppie `host:port` separate da virgola ',', usato nel caso in cui `OuputAdapter` sia `KafkaOutputChannelAdapter`
- **MongoDB.server**: URL per il server MongoDB
- **MongoDB.db.name**: nome del db su cui scrivere i dati
- **input.topic**: nome del topic Kafka da cui leggere i messaggi

## Funzionamento
La classe `ETL_L2` viene istanziata senza richiedere alcun parametro al costruttore, il quale crea un'istanza di 
`ConsumeToDocumentStore`, inizializzato tramite una lista di istanze di `IWriter`, inizializzate a loro volta con due 
implementazioni di `MongoWriter` (seppure con due diverse strategie di scrittura, vedi dopo)

Tramite il metodo `execute`, il consumer si occupa di consumare i messaggi sul topic batch di Kafka, riscrivendoli su 
MongoDB secondo le modalità previste, definite tramite la lista `List<IWriter> writers` (vedi dopo). 

Nel caso in esame, due tipologie di documenti sono richiesti:

- Documento semplice (gestito da `SimpleWriter`)
- Documento elaborato (gestito da `EnhancedWriter`)

```
MongoWriter simpleWriter = new MongoWriter("simple_collection", new SimpleDocumentGenerationStrategy());
MongoWriter enhancedWriter = new MongoWriter("enhanced_collection", new EnhancedDocumentGenerationStrategy());
List<IWriter> writers = new ArrayList<IWriter>();
writers.add(simpleWriter);
writers.add(enhancedWriter);

consumer = new ConsumeToDocumentStore(writers);
```

### MongoWriter
Questa è la classe che si occupa di effettuare la scrittura su MongoDB in modalità `upsert` al fine di evitare duplicati.
 
Essa utilizza il pattern __Strategy__ per definire, senza variare l'implementazione di `MongoWriter`, differenti 
modalità di generazione dei documenti da scrivere.

```
Document obj = Document.parse(recordValue);
...
obj = this.documentGenerationStrategy.process(obj, this.collection);
```

### IDocumentGenerationStrategy
Interfaccia per realizzare la manipolazione del documento da scrivere su MongoDB
```
public interface IDocumentGenerationStrategy {
    public Document process(Document obj, MongoCollection collection);
}
```
L'unico metodo definito, `process` richiede due parametri
- Il documento di partenza
- La collection di destinazione, che può essere usata per recuperare informazioni aggiuntive necessarie all'elaborazione

### SimpleDocumentGenerationStrategy
Genera il documento senza effettuare ulteriori elaborazioni.

### EnhancedDocumentGenerationStrategy
Genera il documento calcolando due ulteriori campi `DeltaOdometer` e `DeltaLifeConsumption`, recuperando da MongoDB, 
mediante opportuna query, le informazioni per calcolare la differenza sui valori attuali quelli immediatamente 
precedenti, noti per lo stesso veicolo.
