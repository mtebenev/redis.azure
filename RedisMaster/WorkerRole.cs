using System;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using StackExchange.Redis;

namespace RedisMaster
{
	public class WorkerRole : RoleEntryPoint
	{
		public override void Run()
		{
			Trace.TraceInformation("Redis Server entry point called");

			bool isRedisStarted = false;
			bool isNeedStartRedis = IsStartRedisServer();

			if(isNeedStartRedis)
			{
				Trace.TraceInformation("Starting up Redis server.");
				isRedisStarted = StartRedisSafe();
			}

			while(true)
			{
				if(isNeedStartRedis && !isRedisStarted)
					Trace.TraceError("Failed to start Redis server.");

				Thread.Sleep(TimeSpan.FromMinutes(1));
			}
		}

		public override void OnStop()
		{
			ShutDownRedis();
			base.OnStop();
		}

		/// <summary>
		/// Starts redis server.
		/// Design note: we do not throw exception from this method to let role keep running and put diagnostic messages to trace.
		/// Otherwise if role fails to start or throws exception from Run(), the role will by 'cycling' - hard to determine what's going wrong.
		/// </summary>
		private bool StartRedisSafe()
		{
			bool result = false;

			try
			{
				string roleRoot = Environment.GetEnvironmentVariable("RoleRoot");

				if(String.IsNullOrEmpty(roleRoot))
					throw new InvalidOperationException("Cannot get role root path.");

				Trace.TraceInformation("roleroot1: {0}|", roleRoot);

				roleRoot = Path.GetFullPath(roleRoot); // On azure server RoleRoot is usually 'F:' (current path on 'F' drive) and for emulator it's real path ending with backslash - this fixes inputs for Path.Combine()

				Trace.TraceInformation("roleroot2: {0}|", roleRoot);
				
				string redisRoot = RoleEnvironment.IsEmulated 
					? Path.Combine(roleRoot, "approot", "redis")
					: Path.Combine(roleRoot, "redis");

				string redisServerPath = Path.Combine(redisRoot, "redis-server.exe");

				Trace.TraceInformation("Redis server directory: {0}|", redisRoot);
				Trace.TraceInformation("Redis server executable: {0}|", redisServerPath);

				Process process = new Process();

				process.StartInfo.FileName = redisServerPath;
				process.StartInfo.WorkingDirectory = redisRoot;

				process.StartInfo.Arguments = "redis.windows.conf";
				process.StartInfo.RedirectStandardInput = true;
				process.StartInfo.RedirectStandardError = true;
				process.StartInfo.RedirectStandardOutput = true;
				process.StartInfo.UseShellExecute = false;
				process.StartInfo.CreateNoWindow = true;
				process.EnableRaisingEvents = false;
				process.OutputDataReceived += new DataReceivedEventHandler(processToExecuteCommand_OutputDataReceived);
				process.ErrorDataReceived += new DataReceivedEventHandler(processToExecuteCommand_ErrorDataReceived);
				process.Start();

				process.BeginOutputReadLine();
				process.BeginErrorReadLine();

				result = true;
			}
			catch(Exception e)
			{
				Trace.TraceError(e.ToString());
			}

			return result;
		}

		/// <summary>
		/// We need to shut down redis server to safely re-deploy the worker
		/// </summary>
		private void ShutDownRedis()
		{
			try
			{
				// Try obtaining redis port from configuration
				int redisPort = 6379;

				string redisPortString = ConfigurationManager.AppSettings["RedisPort"];
				if(!String.IsNullOrEmpty(redisPortString))
					Int32.TryParse(redisPortString, out redisPort);

				ConfigurationOptions configOptions = new ConfigurationOptions();
				configOptions.EndPoints.Add(new DnsEndPoint("localhost", redisPort)); // Do not connect to the server, just shut down
				configOptions.AllowAdmin = true;
				configOptions.AbortOnConnectFail = false;

				// Connect and shutdown
				using(ConnectionMultiplexer connectionMultiplexer = ConnectionMultiplexer.Connect(configOptions))
				{
					IServer server = connectionMultiplexer.GetServer("localhost", redisPort);
					if(server == null)
						throw new InvalidOperationException("Cannot obtain redis server instance.");

					server.Shutdown(ShutdownMode.Default, CommandFlags.None);
					connectionMultiplexer.Close();
				}
			}
			catch(Exception e)
			{
				Trace.TraceError("Error during redis role shut down:");
				Trace.TraceError(e.ToString());
			}
		}

		/// <summary>
		/// Checks 'DisableRedisStart' option. Not launching redis server may be useful in emulated environment.
		/// </summary>
		private bool IsStartRedisServer()
		{
			string setting = CloudConfigurationManager.GetSetting("DisableRedisStart");
			bool result = !String.Equals(setting, "true", StringComparison.InvariantCultureIgnoreCase);

			return result;
		}

		private void processToExecuteCommand_ErrorDataReceived(object sender, DataReceivedEventArgs e)
		{
			Trace.WriteLine(e.Data, "Information");
		}

		private void processToExecuteCommand_OutputDataReceived(object sender, DataReceivedEventArgs e)
		{
			Trace.WriteLine(e.Data, "Information");
		}
	}
}
