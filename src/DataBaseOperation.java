import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DataBaseOperation {

    private static final String _dbURL = "jdbc:postgresql://localhost:5432/apiDB";                          //адрес БД
    private static final String _userName = "postgres";                                                     //имя для доступа к БД
    private static final String _userPassword = "qwerty123cvbn";                                            //пароль для доступа к БД



    /**
     * Функция для анализа полей файла ".json",
     * c добавлением параметров в словарь в виде пар "ключ" : "значение"
     */
    public static void parsingJson (String initPath, JsonNode node, Map<String, String> columns) {

        if (node.isObject()) {                                                                          //если узел - объект, то проходимся по его полям

            node.fieldNames().forEachRemaining(field -> {                                               //для каждого поля выполняем лямбда-функцию
                String newPath = initPath.isEmpty() ? field : initPath + "_" + field;                   //формируем новый путь для текущего значения
                parsingJson(newPath, node.get(field), columns);                                         //рекурсивно вызываем функцию для обработки вложенных структур
            });

        } else if (node.isArray()) {                                                                    //если узел - массив, то проходимся по его элементам

            for (int i = 0; i < node.size(); i++) {                                                     //проходимя по всем элементам json

                String newPath = initPath;                                                              //формируем новый путь для текущего значения
                parsingJson(newPath, node.get(i), columns);                                             //рекурсивно вызываем функцию для обработки вложенных структур

            }

        } else if (node.isValueNode()) {                                                                //если узел - конечная пара ("ключ" : "значение")

            if (!initPath.equals("id")) {                                                               //если ключ не "id"

                columns.put(initPath, assignDataType(node));                                            //добавляем значения в словарь, с предварительным присвоением типа данных вместо значения

            } else {

                columns.put(initPath, "INT PRIMARY KEY");                                               //иначе создаем пару id : INT PRIMARY KEY

            }

        }

    }



    /**
     * Функция для проверки значения конечного узла ("ключ" : "значение")
     * и присвоения необходимого типа
     */
    private static String assignDataType (JsonNode node) {

        if (node.isTextual()) {                                                         //если значение - строка

            return "TEXT";                                                              //пришлось заменить на тип "TEXT", так как данные не помещались в "VARCHAR(255)"    :)
            //return "VARCHAR(255)";                                                    //возвращаем строку с типом "VARCHAR(255)" соответственно

        } else if (node.isInt()) {                                                      //если значение - целое число

            return "INT";                                                               //возвращаем строку с типом "INT" соответственно

        } else if (node.isDouble() || node.isFloat()) {                                 //если значение - дробное число (с плавающей точкой)

            return "FLOAT";                                                             //возвращаем строку с типом "FLOAT" соответственно

        } else if (node.isBoolean()) {                                                  //если значение - булевая переменная

            return "BOOLEAN";                                                           //возвращаем строку с типом "BOOLEAN" соответственно

        }

        return "TEXT";          //если ни одно из условий не выполняется, то перед нами текст, возвращаем строку с типом "TEXT" соответственно

    }



    /**
     * Функция для создания таблицы в БД
     * на основании предварительно сформированного словаря
     */
    public static void createTable(Map <String, String> columns) throws Exception {

        try (Connection connection = DriverManager.getConnection(_dbURL, _userName, _userPassword)) {                   //создаем экземпляр для подключения к БД

            Statement statement = connection.createStatement();                                                         //создаем экземпляр для выполнения SQL-запросов
            StringBuilder tableCreateRequest = new StringBuilder("CREATE TABLE IF NOT EXISTS jsonData (");               //создаем экземпляр для "наращивания" тела запроса в виде строки
            columns.forEach((key, value) -> tableCreateRequest.append(key).append(" ").append(value).append(", "));     //обрабатываем каждую пару из словаря, добавляя значения в тело запроса (при помощи лямбда-функции)
            tableCreateRequest.setLength(tableCreateRequest.length() - 2);                                              //убираем последнюю запятую из запроса, так как элементы словаря закончились
            tableCreateRequest.append(");");                                                                            //закрываем тело SQL-запроса

            statement.execute(tableCreateRequest.toString());                                                           //выполняем запрос на создание таблицы в БД
            System.out.println("Таблица успешно создана!");                                                             //если ошибок в процессе создания таблицы не было, то выводим сообщение об успешном выполнении
            System.out.print("\n");

        }

    }



    /**
     * Функция для заполнения предварительно созданной таблицы
     * данными из файла .json
     */
    public static void fillingTableFromJSON(Map <String, String> columns, JsonNode data) throws Exception {

        try (Connection connection = DriverManager.getConnection(_dbURL, _userName, _userPassword)) {

            //вставка и перезапись данных по первичному ключу "id", если требуется (запрос на вставку без дублирования записей)
            StringBuilder tableFillingRequest = new StringBuilder("INSERT INTO jsonData (");                            //создаем экземпляр для "наращивания" тела запроса в виде строки
            columns.keySet().forEach(key -> tableFillingRequest.append(key).append(", "));                              //указываем столбцы для заполнения, путем перечисления всех ключей из словаря (используем лямбда-функцию)
            tableFillingRequest.setLength(tableFillingRequest.length() - 2);                                            //убираем последнюю запятую, так как дальше будут перечислены передаваемые в запрос значения
            tableFillingRequest.append(") VALUES (");                                                                   //переходим к заполнению значений
            columns.keySet().forEach(key -> tableFillingRequest.append("?, "));                                         //создаем временный текстовый шаблон (заглушку) в виде "?", который в дальнейшем будет перезаписан на актуальную информацию (используем лямбда-функцию)
            tableFillingRequest.setLength(tableFillingRequest.length() - 2);                                            //убираем последнюю запятую из запроса
            tableFillingRequest.append(") ON CONFLICT (id) DO UPDATE SET ");                                            //закрываем тело SQL-запроса

            // Добавляем параметры для обновления
            columns.keySet().forEach(key -> tableFillingRequest.append(key).append(" = EXCLUDED.").append(key).append(", "));
            tableFillingRequest.setLength(tableFillingRequest.length() - 2);                                            //убираем последнюю запятую, так как это конец запроса

            PreparedStatement preparedStatement = connection.prepareStatement(tableFillingRequest.toString());          //создаем экземпляр для выполнения "подготовленного" SQL-запроса (шаблон для заполнения данными)

            String path = "";
            Map<String, JsonNode> jsonValues = new HashMap<>();
            //JsonNode jsonData = data;

            ObjectNode changeValueId = (ObjectNode) data;
            if (changeValueId.has("id")) {

                String strValue = changeValueId.get("id").asText();
                int intValue = Integer.parseInt(strValue);
                changeValueId.put("id", intValue);
                //System.out.println(data.get("id").isInt());

            }

            parsingJson1(path, data, jsonValues);

            int i = 1;

            for (String key : columns.keySet()) {                                                                       //перебираем ключи

                JsonNode value = jsonValues.get(key);                                                                   //берем значение текущего ключа

                if (value == null) {                                                                                    //если значение отсутствует

                    preparedStatement.setNull(i, java.sql.Types.NULL);                                                  //устанавливаем "null"

                } else if (value.isTextual()) {                                                                         //если значение - строка

                    preparedStatement.setString(i, value.asText());                                                     //вставяем значение как строку, по текущему индексу в подготовленный запрос

                } else if (value.isInt()) {                                                                             //если значение - целое число

                    preparedStatement.setInt(i, value.asInt());                                                         //вставяем значение как целое число, по текущему индексу в подготовленный запрос

                } else if (value.isDouble() || value.isFloat()) {                                                       //если значение - дробное число (с плавающей точкой)

                    preparedStatement.setDouble(i, value.asDouble());                                                   //вставяем значение как дробное число (с плавающей точкой), по текущему индексу в подготовленный запрос

                } else if (value.isBoolean()) {                                                                         //если значение - булевая переменная

                    preparedStatement.setBoolean(i, value.asBoolean());                                                 //вставяем значение как булевую переменную, по текущему индексу в подготовленный запрос

                } else {

                    preparedStatement.setNull(i, java.sql.Types.NULL);                                                  //если значение не распознано, обрабатываем случай по умолчанию

                }

                i++;

            }

            preparedStatement.executeUpdate();

        }

    }



    /**
     * Функция для анализа полей файла ".json",
     * c добавлением параметров в словарь в виде пар "ключ" : "значение", но обрабатывает словарь,
     * который содержит в себе объект JasonNode,
     * данный объект нужен для дальнейшего анализа каждого поля типа "значение"
     */
    public static void parsingJson1 (String initPath, JsonNode node, Map<String, JsonNode> value) {

        if (node.isObject()) {                                                                              //если узел - объект, то проходимся по его полям

            node.fieldNames().forEachRemaining(field -> {                                                   //для каждого поля выполняем лямбда-функцию
                String newPath = initPath.isEmpty() ? field : initPath + "_" + field;                       //формируем новый путь для текущего значения
                parsingJson1(newPath, node.get(field), value);                                              //рекурсивно вызываем функцию для обработки вложенных структур
            });

        } else if (node.isArray()) {                                                                        //если узел - массив, то проходимся по его элементам

            for (int i = 0; i < node.size(); i++) {

                String newPath = initPath;                                                                  //формируем новый путь для текущего значения
                parsingJson1(newPath, node.get(i), value);                                                  //рекурсивно вызываем функцию для обработки вложенных структур

            }

        } else if (node.isValueNode()) {                                                                    //если узел - конечная пара ("ключ" : "значение")

            value.put(initPath, node);                                                                      //добавляем значения в словарь

        }
    }

}
