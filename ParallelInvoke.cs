using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;

namespace PQuery
{
    public class ParallelInvoke
    {
        private string SQL2014Conn { get; set; }
        private string SQL2012Conn { get; set; }
        private string query { get; set; }
        private int queryTimeout { get; set; }

        private int synchronize { get; set; }
        public DataTable tbl { get; set; }
        private string executionmode { get; set; }
        private System.Threading.CancellationTokenSource cts { get; set; }
        private System.Threading.CancellationToken ct { get; set; }

        public SqlDataReader reader { get; set; }
        public int resultSetCount { get; set; }
        public ParallelInvoke(string Query, string SQL2014Conn, string SQL2012Conn, int QueryTimeout)
        {
            var Appsettings = ConfigurationManager.AppSettings;
            this.SQL2014Conn = SQL2014Conn;
            this.SQL2012Conn = SQL2012Conn;
            this.queryTimeout = QueryTimeout;
            this.query = Query;
            this.cts = new System.Threading.CancellationTokenSource();
            this.ct = cts.Token;

        }
        /*method used to execute query.  executionType can be "SQL2014", "SQL2012", or "Multi"
          "SQL2014 or SQL2012"  will execute in single threaded mode, using respective connection string associated with SQL version         
          "Multi" execution type instructs method to execute both SQL2014 and SQL2012 in parallel returning whichever
           one returns first.
           "SQL2014" and "SQL2012" connection strings are defined in the application config file
        */
        public void executequery(string executionType)
        {
            synchronize = 0;
            this.executionmode = executionType;
            //parallel execution of both sql versions
            if (executionType == "multi")
            {
                QueryTask(SQL2014Conn, SQL2012Conn, query, ct, cts, queryTimeout).Wait();
                //execute cts.Cancel to use ConnectionTokenSource object to cancel 2nd thread
                //  cts.Cancel();
            }
            /*If "SQL2014" or "SQL2012", indicates single threaded execution.  
              Bypass parallel execution, and go straight to "runquery" method
              "runquery" is also called by parallel task as well 
             */
            if (executionType == "SQL2014")
            {
                SqlConnection conn = new SqlConnection(SQL2014Conn);
                SqlCommand cmd = new SqlCommand(query, conn);
                runquery(conn, cmd, query, queryTimeout);
            }
            if (executionType == "SQL2012")
            {
                SqlConnection conn = new SqlConnection(SQL2012Conn);
                SqlCommand cmd = new SqlCommand(query, conn);
                runquery(conn, cmd, query, queryTimeout);
            }
        }

        /* Parallel exeuction initiated by QueryTask method below.  
           Use of CancellationToken and CancellationTokenSource to enable cancellation
           of whichever query takes longer
         */
        private async Task<string> QueryTask(string SQL2014Conn, string SQL2012Conn, string query, System.Threading.CancellationToken ct,
           System.Threading.CancellationTokenSource cts, int queryTimeout)

        {

            Task.Run<string>(() => ManageParallelExecution(SQL2014Conn, query, ct, cts, queryTimeout), ct);
            Task.Run<string>(() => ManageParallelExecution(SQL2012Conn, query, ct, cts, queryTimeout), ct);
            

        }
        //Parallel task execution controlled by ManageParallelExecution  
        //   private static string ManageParallelExecution(string connection, string query,
        //       System.Threading.CancellationToken ct, System.Threading.CancellationTokenSource cts, int queryTimeout)
        private string ManageParallelExecution(string connection, string query,
            System.Threading.CancellationToken ct, System.Threading.CancellationTokenSource cts, int queryTimeout)
        {
            try
            {
                SqlConnection conn = new SqlConnection(connection);
                SqlCommand cmd = new SqlCommand(query, conn);

                //Need to register with SQLCommand above to enable SQLCommand to be cancelled when thread is cancelled
                cts.Token.Register(() => cmd.Cancel());
                //    runquery(conn, cmd, query,queryTimeout);
                this.runquery(conn, cmd, query, queryTimeout);
                //cts.Cancel();
                return "success: " + DateTime.Now.ToString() + ":" + executionmode + ":" + query.Substring(1, 250);

            }
            catch (Exception ex)
            {
                var Appsettings = ConfigurationManager.AppSettings;
                logquery(connection, "Fail :" + DateTime.Now.ToString() + ":" +
                      executionmode + ":" + query.Substring(0, 250) + " :" + ex.Message.Replace("'", ""));
                return "fail";
            }

        }
        //Synchronous query execution runquery called by both Synch and Asynch query calls
        private void runquery(SqlConnection conn, SqlCommand cmd, string query, int queryTimeout)
        {

            try
            {
                //    var Appsettings = ConfigurationManager.AppSettings;
                //    int queryTimeout = Convert.ToInt32(Appsettings["queryTimeout"]);

                SqlDataReader rdr;
                cmd.CommandTimeout = queryTimeout;
                conn.Open();
                rdr = cmd.ExecuteReader();


                resultSetCount = 0;


                //Call logquery.  Will only write to file if "debug" is "true"
                while (synchronize == 1)
                {
                    System.Threading.Thread.Sleep(1000);
                }

                if (synchronize == 0 && tbl == null)
                {
                    synchronize = 1;
                    tbl = new DataTable("Customers");

                    //Load DataReader into the DataTable.


                    if (cts.Token.CanBeCanceled == true)
                    {
                        cts.Cancel();
                    }
                }
                logquery(conn.ConnectionString, "Success :" + DateTime.Now.ToString() + ":" + executionmode + ":" + query.Substring(0, 250));

            }
            catch (Exception ex)
            {
                //Call logquery.  Will only write to file if "debug" is "true"
                logquery(conn.ConnectionString, "Fail :" + DateTime.Now.ToString() + ":" +
                 executionmode + ":" + query.Substring(0, 250) + " :" + ex.Message.Replace("'", ""));

            }
            //not doing anything with this return type.  Not sure how to re-write to use a void return type

        }

        private static void logquery(string conn, string message)
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

    }
}
