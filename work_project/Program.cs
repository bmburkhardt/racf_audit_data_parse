using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace work_project
{
    class Program
    {
        static void Main(string[] args)
        {
            OracleConnection conn = new OracleConnection();
            conn.ConnectionString = "Data Source=orcl;User Id=System;Password=";
            try
            {
                conn.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            OracleCommand command = new OracleCommand(null, conn);
            DataParser parser = new DataParser();
            parser.parseDataByteArrayChunks(ref conn);
            Console.WriteLine("done");
            
            Console.ReadLine();
        }
    }
}
