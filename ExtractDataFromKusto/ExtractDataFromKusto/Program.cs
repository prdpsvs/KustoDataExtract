using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Xml.Linq;

namespace ExtractDataFromKusto
{
    internal class Program
    {
        [STAThread]
        static void Main(string[] args)
        {
            string clusterUrl = "", DatabaseName = "", AppName = "", folderPath = "";
            DateTime startDate = default(DateTime), EndDate = default(DateTime);
            int minutesChunkSize = 0;

            if (Debugger.IsAttached)
            {
                Console.Write("InDebug Mode");
                clusterUrl = "https://sqlazureeus22.kustomfa.windows.net"; DatabaseName = "sqlazure1"; AppName = "bae227202741"; folderPath = "D:\\\\";
                startDate = new DateTime(2022, 03, 02, 04, 00, 00); EndDate = new DateTime(2022, 03, 02, 22, 00, 00);
                minutesChunkSize = 1090;
            }
            else
            {
                if (args.Length == 0)
                {
                    Console.WriteLine("Please provide Cluster URl, Database Name, App Name, Start Date, End Date, Folder Path and HourChunkSize arguments when using this application");
                    Console.ReadLine();
                    Environment.Exit(0);
                }

                try
                {
                    if (args.Length > 0)
                    {
                        Uri uri;
                        if (args[0].IsNullOrEmpty() || !Uri.TryCreate(args[0], UriKind.Absolute, out uri))
                            throw (new Exception("Cluster URL is either empty or invalid. Please provide a valid url. Expecting value : https://sqlazureeus22.kustomfa.windows.net"));
                        clusterUrl = args[0];

                        if (args[1].IsNullOrEmpty() || !args[1].All(c => Char.IsLetterOrDigit(c)))
                            throw (new Exception("Database Name is either empty or invalid. Please provide a valid value. Expecting value : sqlazure1"));
                        DatabaseName = args[1];

                        if (args[2].IsNullOrEmpty() || !args[1].All(c => Char.IsLetterOrDigit(c)))
                            throw (new Exception("App Name is either empty or invalid. Please provide a valid value. Expecting value : bab3ee1eeb56"));
                        AppName = args[2];

                        if (args[3].IsNullOrEmpty() || !DateTime.TryParse(args[3], out startDate))
                            throw (new Exception("startDate is either empty or invalid. Please provide a valid Start Date. Expecting value : 2022-03-02 04:00:00.0000000"));

                        if (args[4].IsNullOrEmpty() || !DateTime.TryParse(args[4], out EndDate))
                            throw (new Exception("EndDate is either empty or invalid. Please provide a valid End Date. Expecting value : 2022-03-02 14:00:00.0000000"));

                        if (args[5].IsNullOrEmpty() || !Directory.Exists(args[5]))
                            throw (new Exception("Folder Path is either empty or invalid. Please provide a valid Folder Path. Expecting value : D:\\"));
                        folderPath = args[5];

                        if (args[6].IsNullOrEmpty() || !int.TryParse(args[6], out minutesChunkSize))
                            throw (new Exception("Hour Chunk Size is either empty or invalid. Please provide a valid value. Expecting value between 4 and 10"));

                        if (startDate >= EndDate)
                            throw (new Exception("Start Date should be less than end date"));
                    }
                    else
                        throw (new Exception("Please check if you provided Cluster URl, DatabaseName, Start Date, EndDate and FolderPath in Order"));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Clipboard.SetText(ex.ToString());
                    Console.ReadLine();
                    Environment.Exit(1);
                }
            }

            var dateRange = SplitDateRange(Convert.ToDateTime(startDate), Convert.ToDateTime(EndDate), minutesChunkSize: minutesChunkSize).ToList();

            var lastDateRange = dateRange.LastOrDefault();
            if(lastDateRange != null && Convert.ToDateTime(lastDateRange.Item2) > EndDate && EndDate > Convert.ToDateTime(lastDateRange.Item1))
            {
                var latestLastDateTimeRange = Tuple.Create<string, string>(lastDateRange.Item1, String.Format("{0:yyyy-MM-dd HH:mm:ss}", EndDate));
                dateRange.RemoveAt(dateRange.Count - 1);
                dateRange.Add(latestLastDateTimeRange);
            }

            Console.WriteLine("Started Data Export Process for provided inputs:");
            Console.WriteLine($"Cluser URl = {clusterUrl}");
            Console.WriteLine($"Database Name = {DatabaseName}");
            Console.WriteLine($"App Name = {AppName}");
            Console.WriteLine($"Start Date = {String.Format("{0:G}", startDate)}");
            Console.WriteLine($"End Date = {String.Format("{0:G}", EndDate)}");
            Console.WriteLine($"Target Folder Path = {folderPath}");
            Console.WriteLine($"Number of file exported in this run are {dateRange.Count}, calculated based on minute chuck size.");
            Console.WriteLine($"Default minute chunk size is 240. Current run is using is {minutesChunkSize} minutes");
            var exceptions = new ConcurrentQueue<Exception>();
            Parallel.ForEach(
                dateRange,
                new ParallelOptions { MaxDegreeOfParallelism = 10 }, dateValue =>
                {
                    try
                    {

                        var client = Kusto.Data.Net.Client.KustoClientFactory.CreateCslQueryProvider($"{clusterUrl}/{DatabaseName};Fed=true");
                        var query = "let myAppName = '" + AppName + "'; let myStartDate = datetime(" + dateValue.Item1 + "); let myEndDate = datetime(" + dateValue.Item2 + "); let snapshotTelemetryInterval = 1h; let QueueDurationThreshold = 1000; let cleanedAppName = extract(@'(\\w+)', 1, myAppName); let handleEmptyValues = (value: long) { iff(isempty(value) == true, -1, value) }; let toMiliseconds = (Duration: timespan) { tolong(Duration / 1ms) }; let getNodeMemory = (slo: int) { case(slo == 100, 0.2 * 300.0 * 1024.0 * 1024.0, slo == 200, 0.4 * 300.0 * 1024.0 * 1024.0, slo == 300, 0.6 * 300.0 * 1024.0 * 1024.0, slo == 400, 0.8 * 300.0 * 1024.0 * 1024.0, slo >= 500, 1.0 * 300.0 * 1024.0 * 1024.0, -1.0) }; let snapshots = MonAnalyticsDBSnapshot | where sql_instance_name contains cleanedAppName and fabric_service_uri contains \"DWShellDb\" | project SnapshotTimeStamp = bin(PreciseTimeStamp, snapshotTelemetryInterval), slo = iff(physical_compute_state == 'Deactivated', 0, toint(extract(@'(\\d+)', 1, service_level_objective))); let minTelemetryDate = toscalar(snapshots | summarize min(SnapshotTimeStamp)); let maxTelemetryDate = toscalar(snapshots | summarize max(SnapshotTimeStamp)); let sloData = range SnapshotTimeStamp from bin(minTelemetryDate, snapshotTelemetryInterval) to bin(maxTelemetryDate, snapshotTelemetryInterval) step snapshotTelemetryInterval | join kind = leftouter(snapshots) on SnapshotTimeStamp | project TimeInterval = SnapshotTimeStamp , SLO = handleEmptyValues(slo) , NodeMemory = getNodeMemory(handleEmptyValues(slo)); MonDwExecRequests | where(SubmitTime >= myStartDate and SubmitTime < myEndDate) or(SubmitTime < myEndDate and myStartDate <= EndTime) | where AppName has cleanedAppName | where ResourceClass != \"\" and ResourceClass != \"default\" | where StatementType != \"Batch\" and StatementType != \"Execute\" and label != 'health_checker' | where status == 'Completed' | summarize SubmitTime = min(SubmitTime), StartTime = max(StartTime), EndTime = max(EndTime), EndCompileTime = max(EndCompileTime), TotalElapsedTime = max(TotalElapsedTime) , importance = any(importance), StatementType = any(StatementType), ResourceClass = any(ResourceClass), SessionId = any(SessionId), GroupName = any(GroupName) , ClassifierName = any(ClassifierName), ResourceAllocationPercentage = max(ResourceAllocationPercentage), ResultCacheHit = max(ResultCacheHit), label = any(label) by RequestId | extend CompileDuration = toMiliseconds(EndCompileTime - StartTime), SubmitStartDuration = toMiliseconds(StartTime - SubmitTime) | extend QueueDuration = SubmitStartDuration - CompileDuration | extend RunDuration = toMiliseconds(EndTime - StartTime) | extend distributed_request_id = RequestId |mv-expand TimeInterval = range(bin(SubmitTime, snapshotTelemetryInterval), bin(EndTime, snapshotTelemetryInterval), snapshotTelemetryInterval) limit 100000 | extend Status = iff(QueueDuration > QueueDurationThreshold, iif(TimeInterval <= StartTime, 'Queued', 'Running'), 'Running') | project TimeInterval = todatetime(TimeInterval), QueueDuration, SessionId, RequestId, distributed_request_id, GroupName, ResourceAllocationPercentage, ResultCacheHit, label, ResourceClass, ClassifierName, Status, importance, StatementType, SubmitTime, StartTime, EndCompileTime, EndTime, RunDuration | join kind = leftouter(MonVdwQueryOperation) on distributed_request_id | where status == 'Succeeded' | join kind = inner(sloData) on TimeInterval | summarize Memory_Per_Compute = any(getNodeMemory(SLO)), importance = any(importance), StatementType = any(StatementType), ResourceClass = any(ResourceClass), SessionId = any(SessionId), GroupName = any(GroupName) , ClassifierName = any(ClassifierName), ResourceAllocationPercentage = max(ResourceAllocationPercentage), label = any(label) , QueueDuration = max(QueueDuration), RunDuration = any(RunDuration) , any(ResourceClass), SumMemory = sum(query_max_used_memory) , TimeInterval = min(TimeInterval), SubmitTime = min(SubmitTime), StartTime = max(StartTime), EndTime = max(EndTime), EndCompileTime = max(EndCompileTime) by RequestId, SLO, NodeName | summarize Memory_Per_Compute = any(Memory_Per_Compute), TimeInterval = min(TimeInterval), SubmitTime = min(SubmitTime), StartTime = max(StartTime), EndTime = max(EndTime), EndCompileTime = max(EndCompileTime) , importance = any(importance), StatementType = any(StatementType), ResourceClass = any(ResourceClass), SessionId = any(SessionId), GroupName = any(GroupName) , ClassifierName = any(ClassifierName), ResourceAllocationPercentage = max(ResourceAllocationPercentage), label = any(label) , QueueDuration = max(QueueDuration), RunDuration = any(RunDuration) , any(ResourceClass), SumMemoryNode = max(SumMemory), MinMemory = min(todecimal(SumMemory)), MaxMemory = max(todecimal(SumMemory)) by RequestId, SLO | extend Memory_Allocated = (Memory_Per_Compute) * (ResourceAllocationPercentage / 100) | extend Running_UnderAllocated = iif(SumMemoryNode > Memory_Allocated, 1, 0), Running_OverAllocated = iif(SumMemoryNode <= Memory_Allocated, 1, 0) | extend Memory_Allocated_Pct = (1 - (SumMemoryNode / Memory_Allocated)) * 100 | extend UnderAllocated = iif(Memory_Allocated_Pct <= 0.0, Memory_Allocated_Pct, 0.0), OverAllocated = iif(Memory_Allocated_Pct >= 0.0, Memory_Allocated_Pct, 0.0) | extend Skew = 1 - (MinMemory / MaxMemory)";
                        string fileName = $"{folderPath}WLMAnalysis_{AppName}_{String.Format("{0:yyyyMMddHHmmss}", Convert.ToDateTime(dateValue.Item1))}.csv";
                        Console.WriteLine($"Exporting WLM Analysis data between {String.Format("{0:G}", Convert.ToDateTime(dateValue.Item1))} and {String.Format("{0:G}", Convert.ToDateTime(dateValue.Item2))} to  {fileName}");
                        using (IDataReader reader = client.ExecuteQuery(query))
                        using (StreamWriter writer = new StreamWriter(fileName))
                        {
                            reader.WriteAsCsv(true, writer);
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions.Enqueue(ex);
                    }
                }
            );
            if (exceptions.Count < 1)
                Console.WriteLine("Export Completed! Please press any key to exit");

            bool flag = false;
            if (exceptions.Count > 0)
            {
                Console.WriteLine("There seems to be an error while exporting WLMAnalysis Data. Analyzing Kusto Service errors...");
                foreach (var exception in exceptions)
                {
                    if (exception is KustoServicePartialQueryFailureException || exception is KustoServicePartialQueryFailureLimitsExceededException || exception is KustoServicePartialQueryFailureLowMemoryConditionException)
                    {
                        Console.WriteLine($"Please reduce the chuck size and try to export data again");
                        flag = true;
                    }
                    else if (exception is KustoServiceTimeoutException)
                    {
                        Console.WriteLine("One or more requests to Kusto have timed out. Please try again in few minutes");
                        flag = true;
                    }

                }
                if (flag != true)
                {
                    Console.WriteLine("Unable to extract error information.");
                    Console.WriteLine("Please use below error stack trace to identify the issue. Error information is copied to your clipboard");
                    Console.WriteLine("=============================== Stack Trace ==================================");
                    var aggException = new AggregateException(exceptions);
                    Console.WriteLine(aggException.ToString());
                    Clipboard.SetText(aggException.ToString());
                }

            }
            Console.ReadLine();
            Environment.Exit(0);
        }

        public static IEnumerable<Tuple<string, string>> SplitDateRange(DateTime start, DateTime end, int minutesChunkSize)
        {
            DateTime chunkEnd;
            while ((chunkEnd = start.AddMinutes(minutesChunkSize)) < end)
            {
                yield return Tuple.Create(String.Format("{0:yyyy-MM-dd HH:mm:ss}", start), String.Format("{0:yyyy-MM-dd HH:mm:ss}", chunkEnd));
                start = chunkEnd;
            }
            yield return Tuple.Create(String.Format("{0:yyyy-MM-dd HH:mm:ss}", start), String.Format("{0:yyyy-MM-dd HH:mm:ss}", chunkEnd));
        }
    }
}
