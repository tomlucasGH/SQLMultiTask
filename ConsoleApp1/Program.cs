using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using PQuery;


namespace ConsoleApp1
{

    class Program
    {
        
        public static IConfigurationRoot Configuration { get; set; }
        static void Main(string[] args)
        {
            int count = Environment.ProcessorCount;
            var builder = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
            //     Console.WriteLine($"option1 = {Configuration["ConnectionString"]}");
            string conn = Configuration["ConnectionString"];
            string queryString = Configuration["QueryString"];
         //   string querystring = "create table #myids (id int)";
            ParallelInvoke parallelinvoke = new ParallelInvoke(queryString,conn, 0, 10,20);
            parallelinvoke.executequery();

           
            /*
            using (SqlConnection connection = new SqlConnection(Configuration["ConnectionString"]))
            {
                SqlCommand cmd = new SqlCommand(querystring, connection);
                connection.Open();
                // Do work here; connection closed on following line.
                cmd.ExecuteNonQuery();

            }
            */
        }
    }
}
