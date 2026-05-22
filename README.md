# SD Laboratorio 2
 Laboratorio 2, Sistemas Distribuido

## Intengrantes:
- Cristobal Espinoza Cáceres (202273507-4)
- Benjamín Ponce Carrera (202173615-8)
- Álvaro Rojas Valenuela (202273502-3)

### Librerias Go
    "bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
    "strconv"
    "math/rand"
	"sync"
	"time"

    "github.com/rabbitmq/amqp091-go"
    "google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

### Instalación
Cada nodo es contenido en su propio Container Docker. Una ves con Docker Engine + Compose preparado, se disponen los siguientes comandos en una terminal alocado en la raiz del proyecto:

**Protocol Buffer**
No debe ser necesario compilar los protoc, dado que vienen compilado por defecto, en caso de necesitarlo se dispone de la siguiente linea:

- make build-protoc

