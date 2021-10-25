using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;

namespace FileMerge
{
    public class FileMergeClass
    {
        static void Main(string[] args)
        {
            //Variable Decleration 
            string fileFolder = "C:\\FileMerge\\Input";

            DataTable dtcombined = MergeFile(fileFolder);

            //Create or Overwrite data in Combinesd CSV 
            string strFilePath = fileFolder + "\\Combined.csv";

            FileStream fcreate = File.Open(strFilePath, FileMode.Create);

            StreamWriter sw = new StreamWriter(fcreate);
            //headers    
            for (int i = 0; i < 2; i++)
            {
                sw.Write(dtcombined.Columns[i]);
                if (i < 1)
                {
                    sw.Write(",");
                }
            }
            sw.Write(sw.NewLine);
            foreach (DataRow dr in dtcombined.Rows)
            {
                for (int i = 0; i < 2; i++)
                {
                    if (!Convert.IsDBNull(dr[i]))
                    {
                        string value = dr[i].ToString();
                        if (value.Contains(','))
                        {
                            value = String.Format("\"{0}\"", value);
                            sw.Write(value);
                        }
                        else
                        {
                            sw.Write(dr[i].ToString());
                        }
                    }
                    if (i < 1)
                    {
                        sw.Write(",");
                    }
                }
                sw.Write(sw.NewLine);
            }
            sw.Close();
            
        }

        public static DataTable MergeFile(string FolderPath)
        {
            DataTable dt = new DataTable("CombineFinal");
            dt.Columns.Add("Source IP");
            dt.Columns.Add("Environment");

            string pattern = @"\d+$";
            Regex rgx = new Regex(pattern);

            DataTable dtCombine = new DataTable("Combine");
            dtCombine.Columns.Add("Source IP");
            dtCombine.Columns.Add("Environment");
            dtCombine.Columns.Add("SplitIP1");
            dtCombine.Columns.Add("SplitIP2");
            dtCombine.Columns.Add("SplitIP3");
            dtCombine.Columns.Add("SplitIP4");

            dtCombine.Columns[2].DataType = typeof(Int32);
            dtCombine.Columns[3].DataType = typeof(Int32);
            dtCombine.Columns[4].DataType = typeof(Int32);
            dtCombine.Columns[5].DataType = typeof(Int32);

            try
            {
                // Fectch File from folder
                FileInfo[] fileInfo = new DirectoryInfo(FolderPath).GetFiles();
                if ((fileInfo != null) && (fileInfo.Length > 0))
                {
                    //Repeat processing for each file in folder & build table
                    for (int i = 0; i < fileInfo.Length; i++)
                    {
                        if (fileInfo[i].Name != "Combined.csv")
                        {
                            DataTable dtFile = ConvertCSVtoDataTable(fileInfo[i].FullName);
                            string[] fileNameSplitted1 = fileInfo[i].Name.Split(".");
                            string fileName = rgx.Replace(fileNameSplitted1[fileNameSplitted1.Length - 2], "");

                            foreach (DataRow r in dtFile.Rows)
                            {
                                string[] splitip = r[0].ToString().Split(".");
                                dtCombine.Rows.Add(r[0], fileName.Trim(), Int32.Parse(splitip[0]), Int32.Parse(splitip[2]), Int32.Parse(splitip[2]), Int32.Parse(splitip[3]));
                            }
                        }
                        else
                        {
                            DataTable dtFile = ConvertCSVtoDataTable(fileInfo[i].FullName);
                            foreach (DataRow r in dtFile.Rows)
                            {
                                string[] splitip = r[0].ToString().Split(".");
                                dtCombine.Rows.Add(r[0], r[1], Int32.Parse(splitip[0]), Int32.Parse(splitip[2]), Int32.Parse(splitip[2]), Int32.Parse(splitip[3]));
                            }
                        }
                    }
                }

                //Get distinct records 
                dtCombine = dtCombine.DefaultView.ToTable(true, "Source IP", "Environment", "SplitIP1", "SplitIP2", "SplitIP3", "SplitIP4");

                //Sort data in table
                DataView dvCombine = new DataView(dtCombine);
                dvCombine.Sort = "SplitIP1 ASC, SplitIP2 ASC, SplitIP3 ASC, SplitIP4 ASC";

                DataTable dtcombined = dvCombine.ToTable();

                foreach (DataRow r in dtcombined.Rows)
                {
                    dt.Rows.Add(r[0], r[1]);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                string[] headers = sr.ReadLine().Split(',');
                foreach (string header in headers)
                {
                    dt.Columns.Add(header);
                }
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }
            }
            return dt;
        }
    }
}
