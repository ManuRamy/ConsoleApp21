using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;
using Oracle.DataAccess.Types;
using System.Data.SqlClient;
using System.Configuration;

namespace ConsoleApp21
{
    class Program
    {
        static void Main(string[] args)
        {
            // string oradb = @"Data Source=(DESCRIPTION=""(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=testdb3.ihs.com)(PORT=1625)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=MB4DEV)));User Id=metauser;Password=data4usn";


            string sourceCS = "Data Source=(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST =TI-ORACLESERV2)(PORT=1521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = TEST3.TECHINDEX.CO.UK)));Password=v8user;User ID=v8user";
            string destinationCS = ConfigurationManager.ConnectionStrings["DestinationCS"].ConnectionString;
            string sourceTable = ConfigurationManager.AppSettings["Sourcetable"];
            string destinationTable = ConfigurationManager.AppSettings["Destinationtable"];
            string sqlScript = "select * from ";
            sqlScript = string.Concat(sqlScript, sourceTable);

            DateTime startTime = DateTime.Now;

            using (OracleConnection sourceCon = new OracleConnection(sourceCS))
            {
                OracleCommand cmd = new OracleCommand(sqlScript, sourceCon);
                sourceCon.Open();

                using (OracleDataReader rdr = cmd.ExecuteReader())
                {
                    using (SqlConnection destinationCon = new SqlConnection(destinationCS))
                    {
                        using (SqlBulkCopy bc = new SqlBulkCopy(destinationCon))
                        {
                            #region without notify
                            //bc.DestinationTableName = "ROYALTY_PERIOD_TEMP";
                            //destinationCon.Open();
                            //bc.WriteToServer(rdr);
                            #endregion

                            bc.BatchSize = 10000;
                            bc.NotifyAfter = 5000;
                            bc.SqlRowsCopied += (sender, eventArgs) =>
                            {
                                Console.WriteLine(eventArgs.RowsCopied + " loaded....");
                            };
                            bc.DestinationTableName = destinationTable;
                            destinationCon.Open();
                            bc.WriteToServer(rdr);
                        }
                    }
                }
            }
            #region only oracle
            //    try
            //{
            //    conn.ConnectionString = sourceConnString;
            //    OracleCommand cmd = new OracleCommand("select count(*) from ROYALTY_PERIOD", conn);
            //    conn.Open();
            //    //Execute the command and use datareader to display the data
            //    OracleDataReader reader = cmd.ExecuteReader();
            //    while (reader.Read())
            //    {
            //        Console.WriteLine("Total count: " + reader.GetValue(0));
            //    }


            //}
            //catch (Exception ex)
            //{
            //    System.IO.File.AppendAllText("C:\\Oracle9iLog\\Oraclelog.txt", ex.ToString());
            //}
            //finally
            //{
            //    conn.Close();
            //}
            #endregion
            TimeSpan totalTimeTaken = DateTime.Now - startTime;
            Console.WriteLine("TimeTaken for migration in minutes : " + totalTimeTaken.TotalMinutes);
            Console.ReadLine();
        }
    }
}
