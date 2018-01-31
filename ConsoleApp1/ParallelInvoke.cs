using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration.Json;
using System.Data;

namespace PQuery
{
    public class ParallelInvoke
    {
        private string Conn { get; set; }
        private string query { get; set; }
        private int queryTimeout { get; set; }

        private int Threads { get; set; }

        private int synchronize { get; set; }
        public DataTable tbl { get; set; }
        private string executionmode { get; set; }
        private System.Threading.CancellationTokenSource cts { get; set; }
        private System.Threading.CancellationToken ct { get; set; }

        private int numrows { get; set; }
        public SqlDataReader reader { get; set; }
        public int resultSetCount { get; set; }
        public ParallelInvoke(string Query, string Conn, int QueryTimeout, int Threads, int numrows)
        {

        //    var Appsettings = ConfigurationManager.AppSettings;
            this.Conn = Conn;
            this.queryTimeout = QueryTimeout;
            this.query = Query;
            this.Threads = Threads;
            this.numrows = numrows;

        }
        /*method used to execute query.  executionType can be "SQL2014", "SQL2012", or "Multi"
          "SQL2014 or SQL2012"  will execute in single threaded mode, using respective connection string associated with SQL version         
          "Multi" execution type instructs method to execute both SQL2014 and SQL2012 in parallel returning whichever
           one returns first.
           "SQL2014" and "SQL2012" connection strings are defined in the application config file
        */
        public void executequery()
        {
    /* int ActiveThreads= ((IEnumerable<int>)System.Diagnostics.Process.GetCurrentProcess().Threads)
     .OfType<System.Diagnostics.ProcessThread>()
     .Where(t => t.ThreadState == System.Diagnostics.ThreadState.Running)
     .Count();
     */
            while (1 == 1)
            {

                QueryTask().Wait();
                //execute cts.Cancel to use ConnectionTokenSource object to cancel 2nd thread
                //  cts.Cancel();
            }

        }

        /* Parallel exeuction initiated by QueryTask method below.  
           Use of CancellationToken and CancellationTokenSource to enable cancellation
           of whichever query takes longer
         */
        private async Task QueryTask( )

        {
           await Task.WhenAll
  (
      Task.Run(() => ManageParallelExecution(Conn, query)),
      Task.Run(() => ManageParallelExecution(Conn, query)),
      Task.Run(() => ManageParallelExecution(Conn, query)),
      Task.Run(() => ManageParallelExecution(Conn, query)),
           Task.Run(() => ManageParallelExecution(Conn, query)),
      Task.Run(() => ManageParallelExecution(Conn, query)),
            Task.Run(() => ManageParallelExecution(Conn, query)),
      Task.Run(() => ManageParallelExecution(Conn, query)),
            Task.Run(() => ManageParallelExecution(Conn, query)),
      Task.Run(() => ManageParallelExecution(Conn, query))

  );
            return;
        }
        //Parallel task execution controlled by ManageParallelExecution  
        //   private static string ManageParallelExecution(string connection, string query,
        //       System.Threading.CancellationToken ct, System.Threading.CancellationTokenSource cts, int queryTimeout)
        private string ManageParallelExecution(string connection, string query)
        {
            try
            {


                    this.runquery(connection, query, queryTimeout);
                    //cts.Cancel();
                    return "success: " + DateTime.Now.ToString() + ":" + executionmode + ":" + query.Substring(1, 250);
              

            }
            catch (Exception ex)
            {
            //    var Appsettings = ConfigurationManager.AppSettings;
            //    logquery(connection, "Fail :" + DateTime.Now.ToString() + ":" +
                  //    executionmode + ":" + query.Substring(0, 250) + " :" + ex.Message.Replace("'", ""));
                return "fail";
            }

        }
        //Synchronous query execution runquery called by both Synch and Asynch query calls
        private void runquery(string connection, string query, int queryTimeout)
        {

            try
            {
                SqlCommand cmd;
                using (SqlConnection conn = new SqlConnection(connection))
                {
                    cmd = new SqlCommand(query, conn);
                    //    var Appsettings = ConfigurationManager.AppSettings;
                    //    int queryTimeout = Convert.ToInt32(Appsettings["queryTimeout"]);
                    cmd.CommandText = query;
                    cmd.CommandTimeout = queryTimeout;
                    conn.Open();
                    cmd.ExecuteNonQuery();
                    query = "";
                    /*  for (int i = 0; i< numrows; i++)
                      {
                          query += "insert into #myids select " + i.ToString();
                      }
                      */
                    cmd.CommandText = query;
                    cmd.ExecuteNonQuery();
                    //    conn.Close();
                    //  resultSetCount = 0;

                    //     logquery(conn.ConnectionString, "Success :" + DateTime.Now.ToString() + ":" + executionmode + ":" + query.Substring(0, 250));
                }
            }
            catch (Exception ex)
            {
                //Call logquery.  Will only write to file if "debug" is "true"
                //logquery(conn.ConnectionString, "Fail :" + DateTime.Now.ToString() + ":" +
             //    executionmode + ":" + query.Substring(0, 250) + " :" + ex.Message.Replace("'", ""));

            }
            //not doing anything with this return type.  Not sure how to re-write to use a void return type

        }

    /*    private static void logquery(string conn, string message)
        {

            try
            {
                var Appsettings = ConfigurationManager.AppSettings;
                if (Appsettings["debug"] == "true")
                {
                    string logFile = Appsettings["logFile"];
                    System.IO.StreamWriter file = System.IO.File.AppendText(logFile);
                    //  new System.IO.StreamWriter(logFile);
                    file.WriteLine(message);
                    file.Close();
                }
            }
            catch (Exception ex)
            {

            }
        }
        */

    }
}
