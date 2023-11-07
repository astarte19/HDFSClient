using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace HDFSClient
{
    class Program
    {
        static readonly HttpClient client = new HttpClient();

        static async Task Main(string[] args)
        {
            

            while (true)
            {                
                Console.Write("hdfs> ");
                string command = Console.ReadLine();
                string hdfsBaseUrl = $"http://{command.Split(" ")[0]}:{command.Split(" ")[1]}/webhdfs/v1/";
                if (command.StartsWith("mkdir"))
                {
                    string directoryName = command.Split(" ")[1];
                    await CreateDirectory(hdfsBaseUrl, directoryName);
                }
                else if (command.StartsWith("put"))
                {
                    string localFilePath = command.Split(" ")[1];
                    string destFileName = Path.GetFileName(localFilePath);
                    await UploadFile(hdfsBaseUrl, localFilePath, destFileName);
                }
                else if (command.StartsWith("get"))
                {
                    string hdfsFilePath = command.Split(" ")[1];
                    await DownloadFile(hdfsBaseUrl, hdfsFilePath);
                }
                else if (command.StartsWith("append"))
                {
                    string localFilePath = command.Split(" ")[1];
                    string hdfsFilePath = command.Split(" ")[2];
                    await AppendFile(hdfsBaseUrl, localFilePath, hdfsFilePath);
                }
                else if (command.StartsWith("delete"))
                {
                    string hdfsFilePath = command.Split(" ")[1];
                    await DeleteFile(hdfsBaseUrl, hdfsFilePath);
                }
                else if (command == "ls")
                {
                    await ListFiles(hdfsBaseUrl);
                }
                else if (command.StartsWith("cd"))
                {
                    string directoryName = command.Split(" ")[1];
                    hdfsBaseUrl = await ChangeDirectory(hdfsBaseUrl, directoryName);
                }
                else if (command == "lls")
                {
                    //ListLocalFiles();
                }
                else if (command.StartsWith("lcd"))
                {
                    string directoryPath = command.Split(" ")[1];
                    Environment.CurrentDirectory = directoryPath;
                }
                else
                {
                    Console.WriteLine("Invalid command");
                }
            }
        }

        static async Task CreateDirectory(string baseUrl, string directoryName)
        {
            try
            {
                string url = $"{baseUrl}{directoryName}?op=MKDIRS";
                await client.PutAsync(url, null);
                Console.WriteLine($"Directory '{directoryName}' created");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception '{ex.Message}'");
            }
            
        }

        static async Task UploadFile(string baseUrl, string localFilePath, string destFileName)
        {
            string url = $"{baseUrl}{Path.GetFileName(destFileName)}?op=CREATE";
            var response = await client.PutAsync(url, null);
            var locationHeader = response.Headers.Location;
            var fileContent = File.ReadAllBytes(localFilePath);

            using (var content = new ByteArrayContent(fileContent))
            {
                await client.PutAsync(locationHeader, content);
                Console.WriteLine($"File '{destFileName}' uploaded");
            }
        }
        static async Task DownloadFile(string baseUrl, string hdfsFilePath)
        {
            string url = $"{baseUrl}{hdfsFilePath}?op=OPEN";
            var response = await client.GetAsync(url);
            var fileContent = await response.Content.ReadAsByteArrayAsync();
            string localFileName = Path.GetFileName(hdfsFilePath);
            File.WriteAllBytes(localFileName, fileContent);
            Console.WriteLine($"File '{hdfsFilePath}' downloaded as '{localFileName}'");
        }
        static async Task AppendFile(string baseUrl, string localFilePath, string hdfsFilePath)
        {
            // Отправка запроса на получение URL для добавления данных в файл
            string appendUrl = $"{baseUrl}{hdfsFilePath}?op=APPEND";
            var appendResponse = await client.PostAsync(appendUrl, null);
            var appendLocation = appendResponse.Headers.Location;

            // Чтение и отправка данных из локального файла на полученный URL
            var fileContent = File.ReadAllBytes(localFilePath);
            using (var content = new ByteArrayContent(fileContent))
            {
                var appendDataResponse = await client.PostAsync(appendLocation, content);
                Console.WriteLine($"Data appended to file '{hdfsFilePath}'");
            }
        }

        static async Task<string> ChangeDirectory(string baseUrl, string directoryName)
        {
            string url;
            if (directoryName == "..")
            {
                int lastSlashIndex = baseUrl.TrimEnd('/').LastIndexOf('/');
                url = baseUrl.Substring(0, lastSlashIndex) + "/";
            }
            else
            {
                url = $"{baseUrl}{directoryName}/";
            }

            string listStatusUrl = $"{url}?op=LISTSTATUS";
            var response = await client.GetAsync(listStatusUrl);
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"Failed to change directory to '{directoryName}': {response.StatusCode}");
                return baseUrl;
            }

            Console.WriteLine($"Changed directory to '{directoryName}'");
            return url;
        }

        static async Task DeleteFile(string baseUrl, string hdfsFilePath)
        {
            string url = $"{baseUrl}{hdfsFilePath}?op=OPEN";
            await client.DeleteAsync(url);           
            Console.WriteLine($"File '{hdfsFilePath}' deleted");
        }

        static async Task ListFiles(string directoryPath)
        {
            List<string> fileList = new List<string>();

            try
            {
                // Construct the list files request URL
                string listUrl = $"{directoryPath}?op=LISTSTATUS";

                // Create a GET request to list the files in the directory
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(listUrl);
                request.Method = "GET";

                // Send the request and get the response
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();

                // Read the response JSON
                using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                {
                    string responseJson = reader.ReadToEnd();

                    // Parse the JSON response
                    dynamic jsonResponse = Newtonsoft.Json.JsonConvert.DeserializeObject(responseJson);

                    // Extract the file paths from the response
                    foreach (var fileStatus in jsonResponse["FileStatuses"]["FileStatus"])
                    {
                        string filePath = fileStatus["pathSuffix"];
                        fileList.Add(filePath);
                    }
                }

                // Close the response
                response.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred: " + ex.Message);
            }

            foreach(var item in fileList)
            {
                Console.WriteLine(item);
            }
        }
    }
}

