using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Oracle.ManagedDataAccess.Client;
using System.IO.MemoryMappedFiles;

namespace work_project
{
    class DataParser
    {
        public List<DataObject> listDataObjects { get; set; }
        public DateTime _reportDt { get; set; }
        public string _path { get; set; }
        public string _file { get; set; }

        public DataParser()
        {
            listDataObjects = new List<DataObject>();
            _path = "c:\\users\\bryan burkhardt\\desktop\\work_project\\work_project\\";
            _file = "testdata_lg.txt";
            _reportDt = getDate();
        }
        public void parseData()
        {
            int lineNumer = 0;
            try
            {
                using (FileStream fs = new FileStream(_path + _file, FileMode.Open, FileAccess.Read))
                {
                    var fileSize = fs.Length;
                    while (!((lineNumer * 97) >= fileSize - 10))
                    {
                        DataObject dataObject = new DataObject();
                        int offset = lineNumer * 97;

                        //system id
                        for (int i = 0; i < 5; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            dataObject._systemId += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //group id
                        offset = 6;
                        for (int i = 0; i < 8; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            dataObject._groupId += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //user id
                        offset = 15;
                        for (int i = 0; i < 8; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            dataObject._userId += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //id type
                        offset = 24;
                        for (int i = 0; i < 20; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            dataObject._idType += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //emp name
                        offset = 45;
                        for (int i = 0; i < 50; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            dataObject._empName += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }
                        dataObject._systemId = dataObject._systemId.Trim();
                        dataObject._groupId = dataObject._groupId.Trim();
                        dataObject._userId = dataObject._userId.Trim();
                        dataObject._idType = dataObject._idType.Trim();
                        dataObject._empName = dataObject._empName.Trim();
                        listDataObjects.Add(dataObject);
                        lineNumer++;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public void parseData(ref OracleConnection conn)
        {
            OracleCommand command = new OracleCommand(null, conn);
            int lineNumer = 0;
            string system_id = "",
                   group_id = "",
                   user_id = "",
                   id_type = "",
                   emp_name = "";
            string dateString = "TO_DATE('" + _reportDt.Month + "/" + _reportDt.Day + "/" + _reportDt.Year + "','MM/DD/YYYY')";
            try
            {
                using (FileStream fs = new FileStream(_path + _file, FileMode.Open, FileAccess.Read))
                {
                    var fileSize = fs.Length;
                    while (!((lineNumer * 97) >= fileSize - 10))
                    {
                        system_id = "";
                        group_id = "";
                        user_id = "";
                        id_type = "";
                        emp_name = "";
                        int offset = lineNumer * 97;

                        //system id
                        for (int i = 0; i < 5; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            system_id += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //group id
                        offset = (lineNumer * 97) + 6;
                        for (int i = 0; i < 8; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            group_id += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //user id
                        offset = (lineNumer * 97) + 15;
                        for (int i = 0; i < 8; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            user_id += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //id type
                        offset = (lineNumer * 97) + 24;
                        for (int i = 0; i < 20; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            id_type += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }

                        //emp name
                        offset = (lineNumer * 97) + 45;
                        for (int i = 0; i < 50; ++i)
                        {
                            fs.Seek(offset, SeekOrigin.Begin);
                            emp_name += Convert.ToChar(fs.ReadByte());
                            offset++;
                        }
                        system_id = system_id.Trim();
                        group_id = group_id.Trim();
                        user_id = user_id.Trim();
                        id_type = id_type.Trim();
                        emp_name = emp_name.Trim();

                        command.CommandText = "insert into system.racf_group_connections(system_id,group_id,user_id,id_type,emp_name,report_dt) values('" + system_id + "','" + group_id + "','" + user_id + "','" + id_type + "','" + emp_name + "'," + dateString + ")";
                        try
                        {
                            command.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                        lineNumer++;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public void parseDataMMap(ref OracleConnection conn)
        {
            
        }

        public void parseDataByteArray(ref OracleConnection conn)
        {
            byte[] buff = File.ReadAllBytes(_path + _file);
            Console.WriteLine(Convert.ToChar(buff[0]));
            OracleCommand command = new OracleCommand(null, conn);
            int lineNumer = 0;
            string system_id = "",
                   group_id = "",
                   user_id = "",
                   id_type = "",
                   emp_name = "";
            string dateString = "TO_DATE('" + _reportDt.Month + "/" + _reportDt.Day + "/" + _reportDt.Year + "','MM/DD/YYYY')";
            var fileSize = buff.Length;
            while (!((lineNumer * 97) >= fileSize - 10))
            {
                
                system_id = "";
                group_id = "";
                user_id = "";
                id_type = "";
                emp_name = "";
                int offset = lineNumer * 97;

                //system id
                for (int i = 0; i < 5; ++i)
                {
                    system_id += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //group id
                offset = (lineNumer * 97) + 6;
                for (int i = 0; i < 8; ++i)
                {
                    group_id += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //user id
                offset = (lineNumer * 97) + 15;
                for (int i = 0; i < 8; ++i)
                {
                    user_id += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //id type
                offset = (lineNumer * 97) + 24;
                for (int i = 0; i < 20; ++i)
                {
                    id_type += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //emp name
                offset = (lineNumer * 97) + 45;
                for (int i = 0; i < 50; ++i)
                {
                    emp_name += Convert.ToChar(buff[offset]);
                    offset++;
                }
                system_id = system_id.Trim();
                group_id = group_id.Trim();
                user_id = user_id.Trim();
                id_type = id_type.Trim();
                emp_name = emp_name.Trim();

                command.CommandText = "insert into system.racf_group_connections(system_id,group_id,user_id,id_type,emp_name,report_dt) values('" + system_id + "','" + group_id + "','" + user_id + "','" + id_type + "','" + emp_name + "'," + dateString + ")";
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                lineNumer++;
                
            }
        }

        public void parseDataByteArrayChunks(ref OracleConnection conn)
        {
            byte[] buff = File.ReadAllBytes(_path + _file);
            Console.WriteLine(Convert.ToChar(buff[0]));
            OracleCommand command = new OracleCommand(null, conn);
            int lineNumer = 0;
            string system_id = "",
                   group_id = "",
                   user_id = "",
                   id_type = "",
                   emp_name = "",
                   strCommand = "";
            command.CommandText = "insert into system.racf_group_connections(system_id,group_id,user_id,id_type,emp_name,report_dt) with blah as ( ";
            string dateString = "TO_DATE('" + _reportDt.Month + "/" + _reportDt.Day + "/" + _reportDt.Year + "','MM/DD/YYYY')";
            var fileSize = buff.Length;
            while (!((lineNumer * 97) >= fileSize - 10))
            {
                system_id = "";
                group_id = "";
                user_id = "";
                id_type = "";
                emp_name = "";
                int offset = lineNumer * 97;

                //system id
                for (int i = 0; i < 5; ++i)
                {
                    system_id += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //group id
                offset = (lineNumer * 97) + 6;
                for (int i = 0; i < 8; ++i)
                {
                    group_id += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //user id
                offset = (lineNumer * 97) + 15;
                for (int i = 0; i < 8; ++i)
                {
                    user_id += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //id type
                offset = (lineNumer * 97) + 24;
                for (int i = 0; i < 20; ++i)
                {
                    id_type += Convert.ToChar(buff[offset]);
                    offset++;
                }

                //emp name
                offset = (lineNumer * 97) + 45;
                for (int i = 0; i < 50; ++i)
                {
                    emp_name += Convert.ToChar(buff[offset]);
                    offset++;
                }
                system_id = system_id.Trim();
                group_id = group_id.Trim();
                user_id = user_id.Trim();
                id_type = id_type.Trim();
                emp_name = emp_name.Trim();

                lineNumer++;
                if (lineNumer % 100 == 0)
                {
                    command.CommandText += "select '" + system_id + "','" + group_id + "','" + user_id + "','" + id_type + "','" + emp_name + "'," + dateString + "from dual) select * from blah";
                    command.ExecuteNonQuery();
                    command.CommandText = "insert into system.racf_group_connections(system_id,group_id,user_id,id_type,emp_name,report_dt) with blah as ( ";
                }
                else
                {
                    command.CommandText += "select '" + system_id + "','" + group_id + "','" + user_id + "','" + id_type + "','" + emp_name + "'," + dateString + "from dual union all ";
                }
            }
            if(command.CommandText != "insert into system.racf_group_connections(system_id,group_id,user_id,id_type,emp_name,report_dt) with blah as ( ")
            {
                command.CommandText = command.CommandText.Substring(0, command.CommandText.Length - 11);
                command.CommandText += ") select * from blah";
                command.ExecuteNonQuery();
            }
        }

        private DateTime getDate()
        {
            FileInfo fileInfo = new FileInfo(_path + _file);
            return fileInfo.LastWriteTime;
        }
    }
}
