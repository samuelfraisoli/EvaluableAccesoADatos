import org.checkerframework.checker.units.qual.A;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.mariadb.jdbc.export.Prepare;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.PrimitiveIterator;
import java.util.Scanner;

public class GestorDB {
    private String user = "";
    private String password = "";
    private String host = "";
    private String nombreDB = "almacen";
    private Connection connection;
    private Statement statementServidorDB;
    private ResultSet resultSetServidorDB;
    private Statement statementDB;
    private Scanner input = new Scanner(System.in);

   /**
    * Constructor GestorDB
    * - Lanza todas las funciones que conectan la aplicación con la base de datos y la inicializan
    * */
    public GestorDB() {
        conectarAServidorDB();

        if (buscarDbEnServidor() == -1) {
            crearDB();
        }
        conectarADb();

        ArrayList<Producto> arrayListJSON = rellenarArrayListProductosDesdeJSON();
        ArrayList<Producto> arrayListTablaProductos = rellenarArrayListTablaProductos();

        rellenarProductosDB(arrayListJSON, arrayListTablaProductos);
        System.out.println("La base de datos se ha iniciado correctamente.");
        System.out.println("\u001B[94m" + "========================\n" + "\u001B[0m");

        System.out.println("Pulse enter para acceder al menú:");
        input.nextLine();
    }


    /**
     * Método realizarConexion()
     * - Va a conectar solamente al servidor de las bases de datos, no a la base de datos en sí
     */
    public void conectarAServidorDB() {

        String urlServidor = "";

        while (true) {
            try {
                System.out.println("\u001B[94m" + "===== NORTHWIND DB =====" + "\u001B[0m");
                System.out.println("Bienvenido al servidor de bases de datos de Northwind");
                System.out.println("Introduce tu usuario:");
                user = input.nextLine();
                System.out.println("Introduce tu contraseña:");
                password = input.nextLine();
                System.out.println("Introduce el host del servidor:");
                host = input.nextLine();

                urlServidor = "jdbc:mariadb://" + host + "/?user=" + user + "&password=" + password;
                connection = DriverManager.getConnection(urlServidor);
                System.out.println("Conexión exitosa al servidor");

                break;


            } catch (SQLException e) {
                System.out.println("Error al conectar al servidor.");
                System.out.println("Se volverá a intentar la conexión. \n");
            }
        }
    }


    /**
     * FUNCIÓN BUSCARDBENSERVIDOR
     * - Se usa cuando estamos conectados al servidor de bases de datos, va a buscar si existe la base de datos.
     * - Si la base de datos existe, devuelve 1
     * - Si la base de datos no existe, devuelve -1
     * - Si se produce algún error y no puede saber si existe o no, devuelve 0
     */
    public int buscarDbEnServidor() {
        String nombreDB = "almacen";
        int existeDBFlag = 0;

        try {
            //chequea que el objeto connection siga activo (o sea, que sigamos conectados al servidor de DB)
            if (!connection.isClosed()) {
                //Crea un objeto statement y ejecuta la query para buscar las bases de datos. Esta query devolverá todas las bases de datos que tengan ese mismo nombre
                statementServidorDB = connection.createStatement();
                resultSetServidorDB = statementServidorDB.executeQuery("SHOW DATABASES LIKE '" + nombreDB + "'");

                //Si podemos usar el método next(), es que la DB existe, si no podemos, es que no existe
                if (resultSetServidorDB.next()) {
                    // La base de datos existe
                    System.out.println("La base de datos '" + nombreDB + "' existe en el servidor.");
                    existeDBFlag = 1;
                } else {
                    // La base de datos no existe
                    System.out.println("La base de datos '" + nombreDB + "' no existe en el servidor.");
                    existeDBFlag = -1;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSetServidorDB != null) {
                    resultSetServidorDB.close();
                }
                if (statementServidorDB != null) {
                    statementServidorDB.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return existeDBFlag;
    }


    /**
     * FUNCIÓN CREARDB
     * - Crea la base de datos con las tablas
     */
    public void crearDB() {
        try {
            statementServidorDB = connection.createStatement();
            statementServidorDB.execute("CREATE DATABASE almacen;");
            statementServidorDB.execute("CREATE TABLE almacen.Productos ( " +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "nombre VARCHAR(100) NOT NULL, " +
                    "descripcion TEXT NOT NULL, " +
                    "cantidad INT NOT NULL, " +
                    "precio DECIMAL(10, 2) NOT NULL, " +
                    "PRIMARY KEY (id)" +
                    ");"
            );
            statementServidorDB.execute("CREATE TABLE almacen.Empleados ( " +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "nombre VARCHAR(50) NOT NULL, " +
                    "apellido1 VARCHAR(50) NOT NULL, " +
                    "apellido2 VARCHAR(50) NOT NULL, " +
                    "correo VARCHAR(100) NOT NULL, " +
                    "PRIMARY KEY (id)" +
                    ");"
            );
            statementServidorDB.execute("CREATE TABLE almacen.Pedidos ( " +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "id_producto INT NOT NULL, " +
                    "descripcion TEXT, " +
                    "precio_total DECIMAL(10, 2), " +
                    "PRIMARY KEY (id), " +
                    "FOREIGN KEY (id_producto) REFERENCES Productos(id)" +
                    ");"
            );
            statementServidorDB.execute("CREATE TABLE almacen.Productos_Fav ( " +
                    "id INT NOT NULL AUTO_INCREMENT, " +
                    "id_producto INT NOT NULL, " +
                    "PRIMARY KEY (id), " +
                    "FOREIGN KEY (id_producto) REFERENCES Productos(id)" +
                    ");"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * FUNCION CONECTARADB
     * - Conecta a la base de datos
     * - No pide usuario, ni contraseña, ni nombre de la BD. Se supone que el usuario ya los ha metido antes
     */
    public void conectarADb() {
        String urlDB = "";
        System.out.println("Conectando a la DB...");

        //Sobreescribe el objeto connection, y ahora conecta a la base de datos, en vez de al servidor de las BBDD
        //Crea el objeto statementDB, usando la nueva conexión. Es el objeto usaré para meter querys en la base de datos
        urlDB = "jdbc:mariadb://" + host + "/" + nombreDB;
        try {
            connection = DriverManager.getConnection(urlDB, user, password);
            statementDB = connection.createStatement();
            System.out.println("Conexión establecida");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Funcion rellenarArrayListProductosDesdeJSON
     * - Va a la URL donde está el JSON, abre un inputStreamReader y un BufferedReader para leerlo.
     * - Luego convierte los objetos del JSON a objetos de tipo Producto.
     * - Finalmente los mete a un arraylist y lo devuelve
     *
     * @return ArrayList
     */
    public ArrayList<Producto> rellenarArrayListProductosDesdeJSON() {

        //ArrayList donde guardaré los productos que saque del JSON
        ArrayList<Producto> arrayListProductos = new ArrayList<>();

        HttpURLConnection httpUrlConnection = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;

        try {
            //Objeto URL que apunta a la url que le paso por parámetro
            URL url = new URL("https://dummyjson.com/products");
            //Cuando uso el método .openConnection retorna una URLConnection, que luego casteo a HTTPURLConnection (buscar)
            httpUrlConnection = (HttpURLConnection) url.openConnection();
            //Creo un inputStream y un bufferedReader usando la URL como parámetro para que la lean
            inputStreamReader = new InputStreamReader(httpUrlConnection.getInputStream());
            bufferedReader = new BufferedReader(inputStreamReader);
            //Creo un stringbuilder para que vaya creando el String, hago un while que lee linea a linea con el bfreader
            StringBuilder stringBuilder = new StringBuilder();
            while (bufferedReader.ready()) {
                stringBuilder.append(bufferedReader.readLine());
            }
            //Parsea el Json que estaba en formato string y crea un objeto JSONObject
            JSONObject jsonObjectProductos = new JSONObject(stringBuilder.toString());
            //uso el método getJSONArray para coger el array que habia dentro del JSON
            JSONArray jsonArrayProductos = jsonObjectProductos.getJSONArray("products");


            //Creo un for para recorrer el JSONArray, cogiendo las propiedades de cada objeto product
            for (int i = 0; i < jsonArrayProductos.length(); i++) {
                JSONObject jsonProducto = jsonArrayProductos.getJSONObject(i);
                int id = jsonProducto.getInt("id");
                String nombre = jsonProducto.getString("title");
                String descripcion = jsonProducto.getString("description");
                int cantidad = jsonProducto.getInt("stock");
                double precio = jsonProducto.getDouble("price");

                arrayListProductos.add(new Producto(id, nombre, descripcion, cantidad, precio));
            }
            System.out.println("Los datos del JSON se han pasado al arraylist correctamente");

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStreamReader != null) {
                try {
                    inputStreamReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (httpUrlConnection != null) {
                httpUrlConnection.disconnect();
            }

            return arrayListProductos;
        }
    }

    /**
     * FUNCION rellenarArrayListTablaProductos()
     * - Coge cada producto de la tabla, lo convierte en un objeto Producto, y luego rellena un arraylist con todos los que había en la tabla.
     *
     */

    public ArrayList<Producto> rellenarArrayListTablaProductos() {
        ArrayList<Producto> arrayListTablaProductos = new ArrayList<>();
        ResultSet resultSetSelectProductos = null;
        try {
            resultSetSelectProductos = statementDB.executeQuery("SELECT * from almacen.Productos");

            while (resultSetSelectProductos.next()) {
                int id = resultSetSelectProductos.getInt("id");
                String nombre = resultSetSelectProductos.getString("nombre");
                String descripcion = resultSetSelectProductos.getString("descripcion");
                int cantidad = resultSetSelectProductos.getInt("cantidad");
                double precio = resultSetSelectProductos.getDouble("precio");

                arrayListTablaProductos.add(new Producto(id, nombre, descripcion, cantidad, precio));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSetSelectProductos != null) {
                try {
                    resultSetSelectProductos.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }

            return arrayListTablaProductos;
        }
    }


    /**
     * FUNCIÓN RELLENARPRODUCTOSDB
     * - Coge por parámetro dos arraylist de tipo producto. Uno rellenado con los productos del JSON, y otro rellenado con los productos de la tabla productos
     * - Comparará el tamaño de los dos, si son del mismo tamaño recorrerá los dos arraylist e irá eliminando sobreescribiendo los productos de la tabla que sean distintos que los del JSON
     * - Si de distinto tamaño, directamente sobreescribirá la tabla con los productos del JSON.
     */
    public void rellenarProductosDB
    (ArrayList<Producto> arrayListProductosJSON, ArrayList<Producto> arrayListTablaProductos) {
        System.out.println("Se revisará si la tabla tiene los mismos productos que en el JSON, si los productos son distintos, se sobreescribirá la tabla con el JSON");
        PreparedStatement preparedStatementProductos = null;
        try {
            preparedStatementProductos = connection.prepareStatement("INSERT INTO almacen.Productos (id, nombre, descripcion, cantidad, precio) VALUES (?, ?, ?, ?, ?)");


            if (arrayListProductosJSON.size() == arrayListTablaProductos.size()) {
                int contadorFilasModificadas = 0;
                for (int i = 0; i < arrayListProductosJSON.size(); i++) {
                    if (!(arrayListProductosJSON.get(i).equals(arrayListTablaProductos.get(i)))) {

                        preparedStatementProductos.setInt(1, arrayListProductosJSON.get(i).getId());
                        preparedStatementProductos.setString(2, arrayListProductosJSON.get(i).getNombre());
                        preparedStatementProductos.setString(3, arrayListProductosJSON.get(i).getDescripcion());
                        preparedStatementProductos.setInt(4, arrayListProductosJSON.get(i).getCantidad());
                        preparedStatementProductos.setDouble(5, arrayListProductosJSON.get(i).getPrecio());
                        preparedStatementProductos.execute();
                        contadorFilasModificadas++;
                    }
                }
                if (contadorFilasModificadas == 0) {
                    System.out.println("No se ha modificado ninguna fila.");
                } else {
                    System.out.println("Se ha(n) modificado " + contadorFilasModificadas + " fila(s).");
                }
            } else {
                if (arrayListTablaProductos.size() == 0) {
                    System.out.println("La tabla estaba vacía, se poblará con los productos del JSON");
                } else {
                    System.out.println("Hay filas en la tabla, pero no tiene el mismo tamaño que el JSON, se sobreescribirá toda la tabla");
                }
                for (int i = 0; i < arrayListProductosJSON.size(); i++) {
                    preparedStatementProductos.setInt(1, arrayListProductosJSON.get(i).getId());
                    preparedStatementProductos.setString(2, arrayListProductosJSON.get(i).getNombre());
                    preparedStatementProductos.setString(3, arrayListProductosJSON.get(i).getDescripcion());
                    preparedStatementProductos.setInt(4, arrayListProductosJSON.get(i).getCantidad());
                    preparedStatementProductos.setDouble(5, arrayListProductosJSON.get(i).getPrecio());
                    preparedStatementProductos.execute();
                }
                System.out.println("La tabla ha sido poblada");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {

            if (preparedStatementProductos != null) {
                try {
                    preparedStatementProductos.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }


    public void añadirEmpleado() {
        String nombre = "";
        String apellido1 = "";
        String apellido2 = "";
        String correo = "";
        String consultaSQL = "INSERT INTO Empleados (nombre, apellido1, apellido2, correo) VALUES (?, ?, ?, ?)";
        boolean empleadoEncontrado = false;
        PreparedStatement preparedStatementEmpleados = null;
        ResultSet resultSetEmpleados = null;

        System.out.println("Introduce el nombre del empleado: ");
        nombre = input.nextLine();

        System.out.println("Introduce el primer apellido del empleado: ");
        apellido1 = input.nextLine();

        System.out.println("Introduce el segundo apellido del empleado: ");
        apellido2 = input.nextLine();

        System.out.println("Introduce el correo del empleado: ");
        correo = input.nextLine();


        try {
            resultSetEmpleados = statementDB.executeQuery("SELECT nombre, apellido1, apellido2 FROM Empleados");
            System.out.print("Revisando si el usuario ya está en la Base de Datos...");
            while (resultSetEmpleados.next()) {
                String nombreTabla = resultSetEmpleados.getString("nombre");
                String apellido1Tabla = resultSetEmpleados.getString("apellido1");
                String apellido2Tabla = resultSetEmpleados.getString("apellido2");


                if (nombre.equals(nombreTabla) && apellido1.equals(apellido1Tabla) && apellido2.equals(apellido2Tabla)) {
                    empleadoEncontrado = true;
                }
            }
            if (empleadoEncontrado) {
                System.out.println(" El empleado ya está en la Base de Datos.");
            } else {
                System.out.println(" El empleado no estaba en la base de datos.");
                System.out.print("Añadiendo nuevo empleado...");
                preparedStatementEmpleados = connection.prepareStatement(consultaSQL);
                preparedStatementEmpleados.setString(1, nombre);
                preparedStatementEmpleados.setString(2, apellido1);
                preparedStatementEmpleados.setString(3, apellido2);
                preparedStatementEmpleados.setString(4, correo);
                preparedStatementEmpleados.execute();
                System.out.println(" Empleado añadido correctamente.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        finally {
            if (resultSetEmpleados != null) {
                try {
                    resultSetEmpleados.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatementEmpleados != null) {
                try {
                    preparedStatementEmpleados.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }

    }



    public void añadirPedido() {
        int idProducto = 0;
        String descripcion = "";
        double precioTotal = 0;
        String consultaSQL = "INSERT INTO Pedidos (id_Producto, descripcion, precio_total) VALUES (?, ?, ?)";
        PreparedStatement preparedStatementPedidos = null;
        ResultSet resultSetIdProductos = null;
        boolean idProductoEncontrada = false;

        while (true) {
            try {
                System.out.println("Introduce la ID del producto. La ID debe estar en la BD para poder ser introducida:  ");
                idProducto = Integer.parseInt(input.nextLine());

                resultSetIdProductos = statementDB.executeQuery("SELECT id FROM almacen.Productos");
                while (resultSetIdProductos.next()) {
                    if (idProducto == resultSetIdProductos.getInt("id")) {
                        idProductoEncontrada = true;
                    }
                }
                if (idProductoEncontrada) {
                    System.out.println("La ID de producto está en la BD. ID añadida al pedido");
                    break;
                } else {
                    System.out.println("La ID de producto no está en la BD.");

                }
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (NumberFormatException e) {
                System.out.println("Introduce una ID válida");
            }
        }

        System.out.println("Introduce la descripción del pedido: ");
        descripcion = input.nextLine();

        while (true) {
            System.out.println("Introduce el precio total del pedido: ");
            try {
                precioTotal = Double.parseDouble(input.nextLine());
                break;
            } catch (NumberFormatException e) {
                System.out.println("Introduce un valor válido");
            }
        }

        try {
            preparedStatementPedidos = connection.prepareStatement(consultaSQL);
            preparedStatementPedidos.setInt(1, idProducto);
            preparedStatementPedidos.setString(2, descripcion);
            preparedStatementPedidos.setDouble(3, precioTotal);

            preparedStatementPedidos.execute();
            System.out.println("Pedido añadido correctamente.");

        } catch (SQLException e) {
            e.printStackTrace();
        }

        finally {
            if (resultSetIdProductos != null) {
                try {
                    resultSetIdProductos.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatementPedidos != null) {
                try {
                    preparedStatementPedidos.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void mostrarEmpleados() {
        System.out.println("Mostrando la lista de empleados:\n");
        ResultSet resultSetMostrarEmpleados = null;
        try {
            resultSetMostrarEmpleados = statementDB.executeQuery("SELECT id, nombre, apellido1, apellido2, correo FROM Empleados");
            while (resultSetMostrarEmpleados.next()) {
                System.out.println("Empleado nº " + resultSetMostrarEmpleados.getInt("id"));
                System.out.println(resultSetMostrarEmpleados.getString("nombre"));
                System.out.println(resultSetMostrarEmpleados.getString("apellido1"));
                System.out.println(resultSetMostrarEmpleados.getString("apellido2"));
                System.out.println(resultSetMostrarEmpleados.getString("correo"));
                System.out.println("");
            }

            System.out.println("Pulsa enter para volver al menú");
            input.nextLine();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            if (resultSetMostrarEmpleados != null) {
                try {
                    resultSetMostrarEmpleados.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void mostrarPedidos() {
        System.out.println("Mostrando la lista de pedidos:\n");
        ResultSet resultSetMostrarPedidos = null;
        try {
            resultSetMostrarPedidos = statementDB.executeQuery("SELECT * FROM Pedidos");
            while (resultSetMostrarPedidos.next()) {
                System.out.println(" - id: " + resultSetMostrarPedidos.getInt("id"));
                System.out.println(" - id producto: " + resultSetMostrarPedidos.getInt("id_producto"));
                System.out.println(" - descripción: " + resultSetMostrarPedidos.getString("descripcion"));
                System.out.println(" - precio total: " + resultSetMostrarPedidos.getDouble("precio_total"));
                System.out.println("");
            }

            System.out.println("Pulsa enter para volver al menú");
            input.nextLine();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSetMostrarPedidos != null) {
                try {
                    resultSetMostrarPedidos.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void mostrarProductos() {
        System.out.println("Mostrando la lista de productos:\n");
        ResultSet resultSetMostrarProductos = null;
        try {
            resultSetMostrarProductos = statementDB.executeQuery("SELECT * FROM Productos");
            while (resultSetMostrarProductos.next()) {
                System.out.println(" - id: " + resultSetMostrarProductos.getInt("id"));
                System.out.println(" - nombre: " + resultSetMostrarProductos.getString("nombre"));
                System.out.println(" - descripción: " + resultSetMostrarProductos.getString("descripcion"));
                System.out.println(" - cantidad: " + resultSetMostrarProductos.getInt("cantidad"));
                System.out.println(" - precio: " + resultSetMostrarProductos.getDouble("precio"));
                System.out.println("");
            }

            System.out.println("Pulsa enter para volver al menú");
            input.nextLine();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSetMostrarProductos != null) {
                try {
                    resultSetMostrarProductos.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }





    public void mostrarProductosMenos600() {
        System.out.println("Mostrando la lista de productos de menos de 600 euros:\n");
        try (ResultSet resultSetMostrarProductos = statementDB.executeQuery("SELECT * FROM Productos WHERE precio < 600;")) {
            while (resultSetMostrarProductos.next()) {
                System.out.println(" - id: " + resultSetMostrarProductos.getInt("id"));
                System.out.println(" - nombre: " + resultSetMostrarProductos.getString("nombre"));
                System.out.println(" - descripción: " + resultSetMostrarProductos.getString("descripcion"));
                System.out.println(" - cantidad: " + resultSetMostrarProductos.getInt("cantidad"));
                System.out.println(" - precio: " + resultSetMostrarProductos.getDouble("precio"));
                System.out.println("\n");
            }

            System.out.println("Pulsa enter para volver al menú");
            input.nextLine();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void insertarProductosFav() {
        ArrayList<Integer> arrayListProductosMas1000 = new ArrayList<>();
        System.out.println("Añadiendo los productos de más de 1000 euros a productos fav:\n");
        try (ResultSet resultSetMostrarProductos = statementDB.executeQuery("SELECT * FROM Productos WHERE precio > 1000;")) {
            while (resultSetMostrarProductos.next()) {
                int id = resultSetMostrarProductos.getInt("id");
                arrayListProductosMas1000.add(id);
            }

            PreparedStatement pStatementInsertarProdFav = connection.prepareStatement("INSERT INTO productos_fav (id_producto) VALUES (?)");

            for (int i = 0; i < arrayListProductosMas1000.size(); i++) {
                pStatementInsertarProdFav.setInt(1, arrayListProductosMas1000.get(i));
                pStatementInsertarProdFav.execute();
            }

            System.out.println("Tabla poblada con los productos");
            System.out.println("Pulsa enter para volver al menú");
            input.nextLine();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


    public void cerrarConexion() {
        try {
            statementDB.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
