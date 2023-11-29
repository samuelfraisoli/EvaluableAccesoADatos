import java.util.Scanner;

public class Menu {
    public static void main(String[] args) {
        GestorDB migestorDB = new GestorDB();
        System.out.println("Lanzando el menú...\n");
        Scanner input = new Scanner(System.in);
        boolean salir = false;
       do{
            System.out.println( "\u001B[94m" + "===== NORTHWIND DB =====\n" + "\u001B[0m" +
                                "Seleccione una opción:\n" +
                                " 1 - Agregar un empleado\n" +
                                " 2 - Agregar un pedido\n" +
                                " 3 - Mostrar empleados\n" +
                                " 4 - Mostrar pedidos\n" +
                                " 5 - Mostrar productos\n" +
                                " 6 - Mostrar productos de menos de 600 euros\n" +
                                " 7 - Insertar productos de más de 1000 euros en productos_fav\n" +
                                " 8 - Salir\n" +
                                "\u001B[94m" + "========================\n" + "\u001B[0m"
                                );
            String opcion = input.nextLine();
            switch (opcion) {
                case "1":
                    migestorDB.añadirEmpleado();
                    break;
                case "2":
                    migestorDB.añadirPedido();
                    break;
                case "3":
                    migestorDB.mostrarEmpleados();
                    break;
                case "4":
                    migestorDB.mostrarPedidos();
                    break;
                case "5":
                    migestorDB.mostrarProductos();
                    break;
                case "6":
                    migestorDB.mostrarProductosMenos600();
                    break;
                case "7":
                    migestorDB.insertarProductosFav();
                    break;
                case "8":
                    migestorDB.cerrarConexion();
                    salir = true;
                    break;
            }
        } while(!salir);




    }
}
