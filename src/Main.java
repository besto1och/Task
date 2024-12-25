import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Main {

    private static final String _apiURL = "https://budget.gov.ru/epbs/registry/ubpandnubp/data";                        //адрес для запроса к API
    private static final String _zipOutputName = "apiData.zip";                                                         //имя архива для хранения скачанных данных

    public static void main(String[] args) throws Exception {

        Scanner consoleInput = new Scanner(System.in);
        System.out.print("Введите дату в формате 'дд.мм.гггг' которой будет начинаться период: ");
        String minLoadDate = consoleInput.nextLine();
        System.out.print("\n");
        System.out.print("Введите дату в формате 'дд.мм.гггг' которой будет заканчиваться период: ");
        String maxLoadDate = consoleInput.nextLine();
        System.out.print("\n");

        int pageSize = 1;
        boolean isValid = false;

        while (!isValid) {

            System.out.print("Введите количество записей, которое будет отображаться на одной странице (значения от 1 до 100): ");


            if (consoleInput.hasNextInt()) {

                pageSize = consoleInput.nextInt();

                if (0 < pageSize && pageSize <= 100) {

                    isValid = true;

                } else {

                    System.out.print("\n");
                    System.out.println("ОШИБКА: Введеное значение не удовлетворяет диапазону, повторите ввод...");
                    System.out.print("\n");
                    pageSize = 1;

                }

            } else {
                System.out.print("\n");
                System.out.println("ОШИБКА: Введеное значение не является числом, повторите ввод...");
                System.out.print("\n");
                consoleInput.next();

            }

        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault();                                              //создаем объект для выполнения HTTP-запросов (со стандартной конфигурацией HTTP-клиента)
             ZipOutputStream ZipOutput = new ZipOutputStream(new FileOutputStream(_zipOutputName))) {                   //создаем объект для записи данных в архив

            System.out.print("\n");
            System.out.println("Ждите, выполняется запрос...");

            CloseableHttpResponse httpResponse = httpClient.execute(new HttpGet(
                    "https://budget.gov.ru/epbs/registry/ubpandnubp/data" +                                         //создаем и отправляем GET-запрос на сервер, и получаем ответ
                            "?pageSize=" + pageSize + "&filterminloaddate=" +
                            minLoadDate + "&filtermaxloaddate=" + maxLoadDate));

            HttpEntity httpEntity = httpResponse.getEntity();                                                           //создаем объект для извлечения "тела" ответа

            String jsonResponse = EntityUtils.toString(httpEntity, "UTF-8");                                //преобразуем извлеченное ранее "тело" ответа в строку для дальнейшей обработки

            //парсим JSON-строку в дерево узлов и извлекаем значение для ключа "pageCount"
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonResponse);
            int pageCount = rootNode.path("pageCount").asInt();
            System.out.println("По вашему запросу найдено: " + pageCount + " страниц(ы)");
            System.out.print("\n");

            System.out.println("Создание таблицы в БД...");

            JsonNode dataNode = rootNode.get("data");                                                                   //извлекаем массив по ключу "data"
            
            Map<String, String> myColumns = new HashMap<>();
            
            if (dataNode.isArray() && !dataNode.isEmpty()) {                                                            //если блок "data" является массивом и он не пустой
                
                JsonNode firstElement = dataNode.get(0);                                                                //берем первый элемент массива, он станет "скелетом" для создания таблицы в БД
                String myInitPath = "";

                DataBaseOperation.parsingJson(myInitPath, firstElement, myColumns);                                     //функция для формирования словаря из JSON
                DataBaseOperation.createTable(myColumns);                                                               //функция для создания таблицы в БД по сформированному словарю
            
            } else {

                System.out.println("ОШИБКА: Блок 'data' не является массивом или не содержит элементов!");

            }

            for (int page = 1; page <= pageCount; page++) {                                                             //проходимся по всем страницам за указанный период, которые были найдены

                System.out.println("Скачивание данных... (страница: №" + page + " )");                                  //выводим сообщение о начале скачивания новой (текущей) страницы


                String finitApiURL = _apiURL + "?pageNum=" + 
                        page +                                                                                          //собираем в строку новый запрос, так как меняется номер страницы
                        "&pageSize=" + pageSize + "&filterminloaddate=" +
                        minLoadDate + "&filtermaxloaddate=" + maxLoadDate;


                String fileOutputName = "data_page_" + page + ".json";                                                  //собираем в строку название файла в котором будет сохранена скачанная информация

                CloseableHttpResponse httpResponseNEW = httpClient.execute(new HttpGet(finitApiURL));                   //создаем и отправляем новый GET-запрос на сервер, и получаем ответ
                HttpEntity httpEntityNEW = httpResponseNEW.getEntity();                                                 //создаем объект для извлечения "тела" ответа
                String jsonResponse1 = EntityUtils.toString(httpEntityNEW, "UTF-8");                        //преобразуем извлеченное ранее "тело" ответа в строку для дальнейшей обработки

                System.out.println("Данные успешно скачались!");
                System.out.print("\n");

                JsonNode rootNode1 = objectMapper.readTree(jsonResponse1);                                              //парсим JSON-строку в дерево узлов
                JsonNode dataNode1 = rootNode1.get("data");                                                             //извлекаем массив по ключу "data"

                System.out.println("Заполнение таблицы данными со страницы №" + page + "...");

                if (dataNode1.isArray() && !dataNode1.isEmpty()) {                                                      //если блок "data" является массивом и он не пустой

                    for (int i = 0; i < rootNode1.get("data").size(); i++) {                                            //проходимся по элементам массива

                        JsonNode currentElement = dataNode1.get(i);                                                     //берем текущий элемент
                        DataBaseOperation.fillingTableFromJSON(myColumns, currentElement);                              //вызываем функцию для заполнения данными таблицы в БД

                        int record = i + 1;
                        System.out.println("Запись " + record + ", находящаяся на странице №" + page +
                                ", успешно занесена в таблицу!");

                    }

                } else {

                    System.out.println("ОШИБКА: Блок 'data' не является массивом или не содержит элементов!");

                }

                System.out.println("Все данные со страницы №" + page + " были успешно загружены!");

                System.out.print("\n");
                System.out.println("Архивация данных... (страница: №" + page + " )");

                if (!dataNode1.isMissingNode()) {                                                                       //если текущий узел не является "отсутствующим узлом"

                    String getDataJSON = dataNode1.toString();                                                          //сохраняем всю информацию как JSON-строку
                    ZipEntry zipEntry = new ZipEntry(fileOutputName);                                                   //создаем новую запись в ZIP-архиве
                    ZipOutput.putNextEntry(zipEntry);                                                                   //добавляем новую запись в ZIP-архив
                    byte[] data = getDataJSON.getBytes();                                                               //конвертируем строку в массив байтов (кодировка по умолчанию: UTF-8)
                    ZipOutput.write(data, 0, data.length);                                                          //пишем данные в текущую (созданную) запись
                    ZipOutput.closeEntry();                                                                             //закрываем текущую запись

                    System.out.println("Данные сохранены в файл: " + fileOutputName);

                } else {

                    System.out.println("ОШИБКА: Блок 'data' отсутствует в ответе сервера!");

                }

                System.out.println("Все страницы были успешно архивированы!");
                System.out.print("\n");

            }

        }

    }

}