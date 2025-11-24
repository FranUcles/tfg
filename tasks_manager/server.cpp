#include <unistd.h>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <arpa/inet.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <functional>
#include <sys/un.h>
#include "ThreadPool.hpp"
using namespace std;
using namespace nlohmann;

void exec_start(const string& input, const string& output);

unordered_map<string, function<void(string, string)>> commands {
    {"START", exec_start}
};

unordered_map<string, vector<string>> required_fields = {
    {"CONFIG", {"shared_dir", "use_conda", "log_level", "workflow_file", "connection"}},
    {"TCP", {"port"}},
    {"UNIX", {"socket_path"}},
    {"START", {"input", "output"}}
};

const string REQUEST_OK = R"({
        "result": "OK"
    })";

const string REQUEST_FAILURE = R"({
        "result": "FAIL"
    })";

const string ERROR_FLAG = "FAIL";

inline string get_log_flag(const string& log_level){
    if(log_level == "INFO")
        return "";
    if (log_level == "DEBUG")
        return "-d";
    if (log_level == "ERROR")
        return "-q";
    if (log_level == "NO_LOGS")
        return "--no-logs";
    return ERROR_FLAG;
}

int PORT = 0;
string SHARED_DIR = "";
string LISTEN_ADDR = "0";
string LOG_FLAG = "";
string WORKFLOW_FILE = "";
string COMMAND_PREFIX = "";
string SERVER_MODE = "";
string UNIX_PATH = "";


bool is_correct_format(json json, vector<string> required_fields, bool debug = true) {
    bool correct_format = true;
        for (const auto& field : required_fields) {
            if (!json.contains(field)) {
                if (debug)
                    cerr << "En la JSON falta el campo obligatorio: " << field << endl;
                correct_format = false;
            }
        }
    return correct_format;
}

void exec_start(const string& input, const string& output) {
    constexpr const char* command_template = "python {} {} -i {} -o {}";
    ostringstream oss;
    oss << "python " << WORKFLOW_FILE << " " << LOG_FLAG << " -i " << SHARED_DIR + input << " -o " << SHARED_DIR + output;
    string command = COMMAND_PREFIX + oss.str(); 
    int ret = system(command.c_str());
    if(ret != 0) {
        cerr << "Error ejecutando el comando" << endl;
    }
}

ssize_t read_exact(int fd, void* buffer, size_t size) {
    size_t total = 0;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(buffer);

    while (total < size) {
        ssize_t n = read(fd, ptr + total, size - total);
        if (n <= 0) return n;  // error o desconexión
        total += n;
    }
    return total;
}

vector<char> read_payload(int fd, uint32_t payload_len){

    // Leer todo el mensaje completo
    vector<char> data(payload_len);
    if (read_exact(fd, data.data(), payload_len) != (ssize_t)payload_len) {
        cerr << "Error leyendo payload\n";
        return {};
    }
    return data;
}

int64_t handle_client_message(int fd) {
    uint32_t net_len;

    // Leemos cuál es el tamaño del mensaje
    if (read_exact(fd, &net_len, sizeof(net_len)) != sizeof(net_len)) {
        cerr << "Error leyendo longitud o cliente desconectado" << endl;
        return 1;
    }

    // Convertir de network byte order (big endian) a host
    uint32_t len = ntohl(net_len);

    // Validamos que el mensaje no sea inmenso
    if (len == 0 || len > 1024) { 
        cerr << "Longitud inválida: " << len << endl;
        return 1;
    }

    // Leer todo el mensaje completo
    vector<char> data = read_payload(fd, len);
    if (data.empty()) return 1;

    try {
        json details = json::parse(data);
        if (!details.contains("cmd")){
            cerr << "JSON mal formateado: " << details.dump(4) << endl;
            return 1;
        }
        string cmd = details["cmd"];
        if (commands.find(cmd) != commands.end()){
            if (!is_correct_format(details, required_fields[cmd])){
                cerr << "Parámetros del comando " << cmd << " erróneos: " << details.dump(4) << endl;
                return 1;
            }
            commands[cmd](details["input"], details["output"]);
            
        } else {
            cerr << "Comando desconocido: " << cmd << endl;
            return 1;
        }
    } catch (const json::parse_error& e) {
        cerr << "Error parseando JSON: " << e.what() << endl;
        return 1;
    }
    return 0;
}

string read_file(const string& path) {
    ifstream file(path);
    if (!file.is_open()) {
        cerr << "No se pudo abrir el fichero: " << path << endl;
        return "";
    }

    stringstream buffer;
    buffer << file.rdbuf(); // Lee todo el fichero al stringstream

    return buffer.str();    // Convierte a string
}

void set_config(const string& config_file){
    try {
        // Parseamos la configuración
        json config = json::parse(read_file(config_file));
        if (!is_correct_format(config, required_fields["CONFIG"])) exit(EXIT_FAILURE);
        // Guardamos la configuración en sus respectivas variables
        SHARED_DIR = config["shared_dir"];
        LOG_FLAG = get_log_flag(config["log_level"]);
        WORKFLOW_FILE = config["workflow_file"];
        if (LOG_FLAG == ERROR_FLAG){
            cerr << "Nivel de log incorrecto" << endl;
            exit(EXIT_FAILURE);
        }
        if (config["use_conda"]){
            if (!config.contains("conda_env")){
                cerr << "No se ha especificado un entorno de conda" << endl;
                exit(EXIT_FAILURE);
            }
            ostringstream oss;
            oss << "mamba run -n " << (string)config["conda_env"] << " ";
            COMMAND_PREFIX = oss.str();
        }
        // Verificamos ahora que la configuración de la conexión sea correcta
        json connection = config["connection"];
        if (!connection.contains("mode")){
            cerr << "Falta el modo de la conexión" << endl;
            exit(EXIT_FAILURE);
        }
        SERVER_MODE = connection["mode"];
        // Comprobamos que la conexión tiene todos los campos obligatorios según el tipo
        if (!is_correct_format(connection, required_fields[SERVER_MODE])) exit(EXIT_FAILURE);
        if (SERVER_MODE == "TCP"){
            PORT = connection["port"];
            if (connection.contains("listen_addr"))
                LISTEN_ADDR = connection["listen_addr"];
        } else if (SERVER_MODE == "UNIX"){
            UNIX_PATH = connection["socket_path"];
        }
    } catch (const json::parse_error& e) {
        cerr << "Error al leer la configuración: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }
}

int init_tcp_socket(struct sockaddr_in &address){
    int server_fd;
    // 1. Crear socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 2. Configurar dirección
    address.sin_family = AF_INET;
    if (LISTEN_ADDR == "0") // acepta conexiones de cualquier IP
        address.sin_addr.s_addr = INADDR_ANY; 
    else{
        if(inet_pton(AF_INET, LISTEN_ADDR.c_str(), &address.sin_addr) < 0){
            perror("inet_pton failed");
            exit(EXIT_FAILURE);
        }
    }
    address.sin_port = htons(PORT);

    // 3. Bind
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // 4. Listen
    if (listen(server_fd, 3) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }
    cout << "Escuchando en el puerto " << PORT << endl;
    return server_fd;
}

int init_unix_socket(struct sockaddr_un &address){
    int server_fd;
    const char *path = UNIX_PATH.c_str();
    // 1. Crear socket
    unlink(path);  // limpiar socket previo si existe
    if ((server_fd = socket(AF_UNIX, SOCK_STREAM, 0)) == 0){
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // 2. Configurar dirección
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, path);

    // 3. Bind
    if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    // 4. Listen
    if (listen(server_fd, 5) < 0) {
        perror("listen failed");
        exit(EXIT_FAILURE);
    }
    cout << "Escuchando en el socket " << UNIX_PATH << endl;
    return server_fd;
}

inline int init_socket(struct sockaddr_storage &address){
    if (SERVER_MODE == "TCP")
        return init_tcp_socket(*reinterpret_cast<struct sockaddr_in*>(&address));
    if (SERVER_MODE == "UNIX")
        return init_unix_socket(*reinterpret_cast<struct sockaddr_un*>(&address));
    cerr << "Modo de uso desconocido" << endl;
    exit(EXIT_FAILURE);
}

void send_response(int request_result, int client_fd){
    const string response = request_result == 0 ? REQUEST_OK : REQUEST_FAILURE;
    write(client_fd, response.c_str(), response.size());
}

int main(int argc, char *argv[]) {
    if (argc < 2){
        cerr << "Falta el fichero de configuración" << endl;
        exit(EXIT_FAILURE);
    }
    // Establecemos la configuración
    string config_file = argv[1];
    set_config(config_file);

    // Creamos el pool de hilos que gestionarán las peticiones de los clientes
    ThreadPool threads_pool(5);

    // Inicializamos el socket
    struct sockaddr_storage address;
    int addrlen = sizeof(address);
    int server_fd = init_socket(address);
    int client_fd;

    while (1) {
        // Aceptamos la conexión
        if ((client_fd = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept failed");
            continue;
        }
        // Encolamos el manejo de esta petición para el pool de hilos
        // y seguimos procesando peticiones
        threads_pool.enqueue([client_fd]() {
            int result = handle_client_message(client_fd);
            send_response(result, client_fd);
            close(client_fd);
        });
    }
    return 0;
}
