/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.zeppelin_spark_monitoring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark Monitoring Interpreter for Zeppelin.
 */
public class SparkMonitoringInterpreter extends Interpreter {

	private static Logger logger = LoggerFactory.getLogger(SparkMonitoringInterpreter.class);

	private static final String HELP = "Spark Monitoring interpreter:\n"
			+ "General format (in REST api): /<object>/<object_id>\n"
			+ "  - object: types of objects such as: applications, jobs\n" + "  - object_id: id of the object to view\n"
			+ "Commands (example):\n" + "  - /applications: list all running applications\n"
			+ "  - /applications/<application_id>jobs: list all running job of <application_id>\n";

	private static final List<String> COMMANDS = Arrays.asList(
			"help", "applications", "jobs", "stages", "executors",
			"storage/rdd", "logs", "hour", "day", "month", "year");

	public static final String DEFAULT_KEY = "default";
	public static final String DOT = ".";
	public static final String SPARK_MONITORING_HOST = "spark.monitoring.host";
	public static final String SPARK_MONITORING_PORT = "spark.monitoring.port";
	public static final String DEFAULT_SPARK_MONITORING_HOST = DEFAULT_KEY + DOT + SPARK_MONITORING_HOST;
	public static final String DEFAULT_SPARK_MONITORING_PORT = DEFAULT_KEY + DOT + SPARK_MONITORING_PORT;

	static {
		Interpreter.register("spark_monitoring", "spark_monitoring", SparkMonitoringInterpreter.class.getName(),
				new InterpreterPropertyBuilder().add(DEFAULT_SPARK_MONITORING_HOST, "localhost", "The host of Spark")
						.add(DEFAULT_SPARK_MONITORING_PORT, "4040", "The port for Spark Monitoring").build());
	}
	
	private final HashMap<String, Properties> propertiesMap;
	private String host = "localhost";
	private int port = 4040;
	private String prefixKey = "";
	private boolean isMonitoringServerAvailable = true;
	private static Map<String, SparkMonitoringApplication> mapApplication;
	private String sourceDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	private String displayDateTimeFormat = "yyyy/MM/dd HH:mm:ss";
	private String numberFormat = "#.#";
	
	public SparkMonitoringInterpreter(Properties property) {
		super(property);
		prefixKey = DEFAULT_KEY;
		propertiesMap = new HashMap<>();
		mapApplication = new HashMap<>();
	}

	@Override
	public void open() {
		try {
			//get connection properties, split prefix
			for (String propertyKey : property.stringPropertyNames()) {
		      logger.debug("propertyKey: {}", propertyKey);
		      String[] keyValue = propertyKey.split("\\.", 2);
		      if (2 == keyValue.length) {
		        logger.info("key: {}, value: {}", keyValue[0], keyValue[1]);
		        Properties prefixProperties;
		        if (propertiesMap.containsKey(keyValue[0])) {
		          prefixProperties = propertiesMap.get(keyValue[0]);
		        } else {
		          prefixProperties = new Properties();
		          propertiesMap.put(keyValue[0], prefixProperties);
		        }
		        prefixProperties.put(keyValue[1], property.getProperty(propertyKey));
		      }
		    }
			//remove duplicate key
			Set<String> removeKeySet = new HashSet<>();
		    for (String key : propertiesMap.keySet()) {
		    	Properties properties = propertiesMap.get(key);
		        if (!properties.containsKey(SPARK_MONITORING_HOST) || !properties.containsKey(SPARK_MONITORING_PORT)) {
		          logger.error("{} will be ignored. {} and {} is mandatory.",
		              key, SPARK_MONITORING_HOST, SPARK_MONITORING_PORT);
		          removeKeySet.add(key);
		        }
		    }

		    for (String key : removeKeySet) {
		      propertiesMap.remove(key);
		    }
		    
		} catch (Exception e) {
			isMonitoringServerAvailable = false;
			logger.error("Open connection to Spark Monitoring", e);
		}
	}

	@Override
	public void close() {
		// do nothing because spark monitoring is always running
	}
	
	public String getPrefix(String cmd) {
		boolean firstLineIndex = cmd.startsWith("(");
	
	    if (firstLineIndex) {
	      int configStartIndex = cmd.indexOf("(");
	      int configLastIndex = cmd.indexOf(")");
	      if (configStartIndex != -1 && configLastIndex != -1) {
	        return cmd.substring(configStartIndex + 1, configLastIndex);
	      } else {
	        return null;
	      }
	    } else {
	      return DEFAULT_KEY;
	    }
	}
	
	//get host and port from property
	public void getConnectionInformation(String prefix) {
		Properties properties = propertiesMap.get(prefix);
		if(properties != null) {
			host = properties.getProperty(SPARK_MONITORING_HOST);
			port = Integer.parseInt(properties.getProperty(SPARK_MONITORING_PORT));
		}
	}

	@Override
	public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
		logger.info("Run command '{}'", cmd);
		prefixKey = getPrefix(cmd);

	    if (null != prefixKey && !prefixKey.equals(DEFAULT_KEY)) {
	      cmd = cmd.substring(prefixKey.length() + 2);
	    }
	    
	    String restUrl = cmd.trim();
	    getConnectionInformation(prefixKey);

	    logger.info("Run Spark Monitoring REST url '" + restUrl + "'");
	    logger.info("Host {} and Port {}", host, port);

		if (StringUtils.isEmpty(restUrl) || StringUtils.isEmpty(restUrl.trim())) {
			return new InterpreterResult(InterpreterResult.Code.SUCCESS);
		}

		// server is not available
		if (!isMonitoringServerAvailable) {
			return new InterpreterResult(InterpreterResult.Code.ERROR,
					"Problem when connect to Spark Monitoring server, please check your configuration (host, port,...)");
		}

		// Process some specific commands (help)
		if (restUrl.startsWith("/help")) {
			return processHelp(InterpreterResult.Code.SUCCESS, null);
		} else if (!restUrl.startsWith("/")) {
			// wrong syntax
			return processHelp(InterpreterResult.Code.ERROR, "Wrong REST url! See help to correct it.");
		}

		final String[] items = StringUtils.split(restUrl.trim(), "/");
		final int numOfItems = items.length;

		try {
			// get applications
			if (numOfItems >= 1 && "applications".equalsIgnoreCase(items[0])) {
				return getApplications(restUrl);
			}
			// get jobs
			else if (numOfItems >= 1 && "jobs".equalsIgnoreCase(items[0])) {
				return getJobs(restUrl);
			}
			// get stages
			else if (numOfItems >= 1 && "stages".equalsIgnoreCase(items[0])) {
				return getStages(restUrl);
			}

			return processHelp(InterpreterResult.Code.ERROR, "Unknown REST url");
		} catch (Exception e) {
			return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
		}
	}

	@Override
	public void cancel(InterpreterContext interpreterContext) {
		// Nothing to do
	}

	@Override
	public FormType getFormType() {
		return FormType.SIMPLE;
	}

	@Override
	public int getProgress(InterpreterContext interpreterContext) {
		return 0;
	}

	@Override
	public List<String> completion(String s, int i) {
		final List<String> suggestions = new ArrayList<String>();

		if (StringUtils.isEmpty(s)) {
			suggestions.addAll(COMMANDS);
		} else {
			for (String cmd : COMMANDS) {
				if (cmd.toLowerCase().contains(s)) {
					suggestions.add(cmd);
				}
			}
		}

		return suggestions;
	}

	private InterpreterResult processHelp(InterpreterResult.Code code, String additionalMessage) {
		final StringBuffer buffer = new StringBuffer();

		if (additionalMessage != null) {
			buffer.append(additionalMessage).append("\n");
		}

		buffer.append(HELP).append("\n");

		return new InterpreterResult(code, InterpreterResult.Type.TEXT, buffer.toString());
	}

	/**
	 * Get Application information
	 * 
	 * @param applicationId
	 *            id of application, if null then get all applications
	 * @return Result of the get request, it contains a JSON-formatted string
	 */
	private InterpreterResult getApplications(String restUrl) {
		// connect to server and get response
		if (isMonitoringServerAvailable) {
			// data
			List<String> lsItem = new LinkedList<String>();
			// connect to server and get data
			CloseableHttpClient client = HttpClients.createDefault();
			String url = "http://" + host + ":" + port + "/api/v1" + restUrl;
			HttpGet request = new HttpGet(url);
			CloseableHttpResponse response = null;
			try {
				response = client.execute(request);
				int code = response.getStatusLine().getStatusCode();
				if (code == HttpStatus.SC_OK) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"));
						// parse each line
						String line = "";
						while ((line = reader.readLine()) != null) {
							// split by ":"
							String[] items = line.split(":", 2);
							if (items.length >= 2) {
								lsItem.add(items[0].trim().replaceAll("\"", "").replaceAll(",", ""));
								lsItem.add(items[1].trim().replaceAll("\"", "").replaceAll(",", ""));
							}
						}
					}
				}
			} catch (IOException e) {
				return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
			} finally {
				try {
					if (response != null) {
						response.close();
					}
				} catch (IOException e) {
					return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
				}
			}
			// make return result - in TABLE type
			StringBuffer result = new StringBuffer();
			int lsItemLength = lsItem.size();
			// build the table header and create SparkMonitoringApplication
			List<SparkMonitoringApplication> lsApplication = new ArrayList<>();
			SparkMonitoringApplication application = new SparkMonitoringApplication();
			int row = 0;
			for (int i = 0; i < lsItemLength; i++) {
				String key = lsItem.get(i);
				String value = lsItem.get(++i);
				if ("id".equalsIgnoreCase(key)) {
					// start a new application
					application = new SparkMonitoringApplication();
					if (row == 0) {
						// add header in case row = 0
						result.append("Id");
					}
					application.setId(value);
				} else if ("name".equalsIgnoreCase(key)) {
					if (row == 0) {
						result.append("\tName");
					}
					application.setName(value);
				} else if ("startTime".equalsIgnoreCase(key)) {
					if (row == 0) {
						result.append("\tStart Time");
					}
					// convert dateTime to current locale
					String dateTimeFormat = sourceDateTimeFormat;
					SimpleDateFormat dateFm = new SimpleDateFormat(dateTimeFormat, Locale.US);
					try {
						Date dateTime = dateFm.parse(value);
						dateFm = new SimpleDateFormat(displayDateTimeFormat, Locale.getDefault());
						application.setStartTime(dateFm.format(dateTime));
					} catch (ParseException e) {
					}
				} else if ("endTime".equalsIgnoreCase(key)) {
					// add duration
					if (row == 0) {
						result.append("\tDuration");
					}
					// convert dateTime to current locale
					String dateTimeFormat = sourceDateTimeFormat;
					SimpleDateFormat dateFm = new SimpleDateFormat(dateTimeFormat, Locale.US);
					try {
						Date dateTime = dateFm.parse(value);
						dateFm = new SimpleDateFormat(displayDateTimeFormat, Locale.getDefault());
						application.setEndTime(dateFm.format(dateTime));
					} catch (ParseException e) {
					}
				} else if ("sparkUser".equalsIgnoreCase(key)) {
					// no add to header
					application.setSparkUser(value);
				} else if ("completed".equalsIgnoreCase(key)) {
					if (row == 0) {
						result.append("\tCompleted");
					}
					application.setCompleted(Boolean.parseBoolean(value));
					// add to lsApplication
					lsApplication.add(application);
					mapApplication.put(prefixKey, application);
					row++;
				}
			}
			result.append("\n");
			// convert dateTime to Long
			String dateTimeFormat = displayDateTimeFormat;
			SimpleDateFormat dateFm = new SimpleDateFormat(dateTimeFormat, Locale.getDefault());
			// make result table body
			for (SparkMonitoringApplication app : lsApplication) {
				result.append(app.getId()).append("\t").append(app.getName()).append("\t").append(app.getStartTime());
				if (app.getCompleted()) {
					//calculate duration
					Long startTime;
					try {
						startTime = dateFm.parse(app.getStartTime()).getTime();
					} catch (ParseException e) {
						startTime = 0L;
					}
					Long endTime;
					try {
						endTime = dateFm.parse(app.getEndTime()).getTime();
					} catch (ParseException e) {
						endTime = 0L;
					}
					Double duration = (endTime - startTime) / 1000D;
					NumberFormat nf = new DecimalFormat(numberFormat);
					result.append("\t").append(nf.format(duration) + "s");
				} else {
					result.append("\t").append("-");
				}
				result.append("\t").append(app.getCompleted());
				result.append("\n"); // new row
			}

			return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
					result.toString());
		}

		return new InterpreterResult(InterpreterResult.Code.ERROR, "Error get data from server");
	}

	/**
	 * Get job information
	 * 
	 * @param restUrl
	 * @return Result of the get request
	 */
	private InterpreterResult getJobs(String restUrl) throws Exception {
		// connect to server and get response
		if (isMonitoringServerAvailable) {
			// first, get applications if not existed
			if(mapApplication.get(prefixKey) == null) {
				getApplications("/applications");
			}
			if(mapApplication.get(prefixKey) == null) {
				return new InterpreterResult(InterpreterResult.Code.ERROR, "Error get data from server");
			}
			
			// get jobs of first application
			SparkMonitoringApplication app = mapApplication.get(prefixKey);
			String applicationId = app.getId();
			
			// build rest url
			String[] urlItems = StringUtils.split(restUrl, "/");
			// case: /jobs/
			if(urlItems.length == 1) {
				restUrl = "/applications/" + applicationId + restUrl;
			}
			else if(urlItems.length >= 2) {
				String param = urlItems[1].trim();
				//case param in COMMANDS list
				if(COMMANDS.contains(param)) {
					restUrl = "/applications/" + applicationId + "/jobs/";
				}
				//case /jobs/<job_id>
				else {
					restUrl = "/applications/" + applicationId + restUrl;
				}
			}
			
			// data
			List<String> lsItem = new LinkedList<String>();
			// connect to server and get data
			CloseableHttpClient client = HttpClients.createDefault();
			String url = "http://" + host + ":" + port + "/api/v1" + restUrl;
			logger.info(url);
			HttpGet request = new HttpGet(url);
			CloseableHttpResponse response = null;
			try {
				response = client.execute(request);
				int code = response.getStatusLine().getStatusCode();
				if (code == HttpStatus.SC_OK) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"));
						// parse each line
						String line = "";
						while ((line = reader.readLine()) != null) {
							// split by ":"
							String[] items = line.split(":", 2);
							if (items.length >= 2) {
								lsItem.add(items[0].trim().replaceAll("\"", "").replaceAll(",", ""));
								lsItem.add(items[1].trim().replaceAll("\"", "").replaceAll(",", ""));
							}
						}
					}
				}
			} catch (Exception e) {
				logger.info(e.getMessage());
				throw e;
			} finally {
				try {
					if (response != null) {
						response.close();
					}
				} catch (IOException e) {
					logger.info(e.getMessage());
					throw e;
				}
			}
			int lsItemLength = lsItem.size();
			// create SparkMonitoringJob object
			List<SparkMonitoringJob> lsJob = new ArrayList<SparkMonitoringJob>();
			SparkMonitoringJob job = new SparkMonitoringJob();
			for (int i = 0; i < lsItemLength; i++) {
				String key = lsItem.get(i);
				String value = lsItem.get(++i);
				if ("jobId".equalsIgnoreCase(key)) {
					// start a new job
					job = new SparkMonitoringJob();
					job.setId(value);
				} else if ("name".equalsIgnoreCase(key)) {
					job.setName(value);
				} else if ("description".equalsIgnoreCase(key)) {
					job.setDescription(value);
				}  
				else if ("submissionTime".equalsIgnoreCase(key)) {
					// convert dateTime to current locale
					String dateTimeFormat = sourceDateTimeFormat;
					SimpleDateFormat dateFm = new SimpleDateFormat(dateTimeFormat, Locale.US);
					try {
						Date dateTime = dateFm.parse(value);
						dateFm = new SimpleDateFormat(displayDateTimeFormat, Locale.KOREA);
						job.setSubmissionTime(dateFm.format(dateTime));
						job.setSubmisstionTimeMilis(dateTime.getTime());
					} catch (ParseException e) {
					}
				} else if ("completionTime".equalsIgnoreCase(key)) {
					// convert dateTime to current locale
					String dateTimeFormat = sourceDateTimeFormat;
					SimpleDateFormat dateFm = new SimpleDateFormat(dateTimeFormat, Locale.US);
					try {
						Date dateTime = dateFm.parse(value);
						dateFm = new SimpleDateFormat(displayDateTimeFormat, Locale.KOREA);
						job.setCompletionTime(dateFm.format(dateTime));
						job.setCompletionTimeMilis(dateTime.getTime());
					} catch (ParseException e) {
					}
				} else if ("status".equalsIgnoreCase(key)) {
					job.setStatus(value);
				}
				else if ("numTasks".equalsIgnoreCase(key)) {
					job.setNumTasks(Integer.parseInt(value));
				}
				else if ("numCompletedTasks".equalsIgnoreCase(key)) {
					job.setNumCompletedTasks(Integer.parseInt(value));
				}
				else if ("numFailedTasks".equalsIgnoreCase(key)) {
					job.setNumFailedTasks(Integer.parseInt(value));
					//add to list
					lsJob.add(job);
				}
			}
			// check if get job information or get statistic about jobs
			int urlItemLength = urlItems.length;
			if(urlItemLength == 1) {
				return getJobInformation(lsItem, lsJob);
			}
			else if(urlItemLength >= 2) {
				String command = urlItems[1];
				if(!COMMANDS.contains(command)) {
					return getJobInformation(lsItem, lsJob);
				}
				else {
					return getJobStatistic(urlItems, lsJob);
				}
			}
			
		}

		return new InterpreterResult(InterpreterResult.Code.ERROR, "Error get data from server");
	}
	
	/**
	 * Get Stages information
	 * 
	 * @param restUrl
	 * @return Result of the get request, it contains a JSON-formatted string
	 */
	private InterpreterResult getStages(String restUrl) {
		// connect to server and get response
		if (isMonitoringServerAvailable) {
			// first, get applications if not existed
			if(mapApplication.get(prefixKey) == null) {
				getApplications("/applications");
			}
			if(mapApplication.get(prefixKey) == null) {
				return new InterpreterResult(InterpreterResult.Code.ERROR, "Error get data from server");
			}
			
			// get stages of first application
			SparkMonitoringApplication app = mapApplication.get(prefixKey);
			String applicationId = app.getId();
			// build rest url
			restUrl = "/applications/" + applicationId + restUrl;
			
			// data
			List<String> lsItem = new LinkedList<String>();
			// connect to server and get data
			CloseableHttpClient client = HttpClients.createDefault();
			String url = "http://" + host + ":" + port + "/api/v1" + restUrl;
			HttpGet request = new HttpGet(url);
			CloseableHttpResponse response = null;
			try {
				response = client.execute(request);
				int code = response.getStatusLine().getStatusCode();
				if (code == HttpStatus.SC_OK) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"));
						// parse each line
						String line = "";
						while ((line = reader.readLine()) != null) {
							// split by ":"
							String[] items = line.split(":", 2);
							if (items.length >= 2) {
								lsItem.add(items[0].trim().replaceAll("\"", "").replaceAll(",", ""));
								lsItem.add(items[1].trim().replaceAll("\"", "").replaceAll(",", ""));
							}
						}
					}
				}
			} catch (IOException e) {
				return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
			} finally {
				try {
					if (response != null) {
						response.close();
					}
				} catch (IOException e) {
					return new InterpreterResult(InterpreterResult.Code.ERROR, "Error : " + e.getMessage());
				}
			}
			// make return result - in TABLE type
			StringBuffer result = new StringBuffer();
			int lsItemLength = lsItem.size();
			// build the table header and create SparkMonitoringStage
			// list
			List<SparkMonitoringStage> lsStage = new ArrayList<>();
			SparkMonitoringStage stage = new SparkMonitoringStage();
			for (int i = 0; i < lsItemLength; i++) {
				String key = lsItem.get(i);
				String value = lsItem.get(++i);
				if ("status".equalsIgnoreCase(key)) {
					// start a new stage
					stage = new SparkMonitoringStage();
					stage.setStatus(value);
				} 
				else if ("stageId".equalsIgnoreCase(key)) {
					stage.setId(value);
				}
				else if ("numCompleteTasks".equalsIgnoreCase(key)) {
					stage.setNumCompletedTasks(Integer.parseInt(value));
				} 
				else if ("numFailedTasks".equalsIgnoreCase(key)) {
					stage.setNumFailedTasks(Integer.parseInt(value));
				} 
				else if ("inputBytes".equalsIgnoreCase(key)) {
					stage.setInputBytes(Long.parseLong(value));
				} 
				else if ("inputRecords".equalsIgnoreCase(key)) {
					stage.setInputRecords(Long.parseLong(value));
				} 
				else if ("outputBytes".equalsIgnoreCase(key)) {
					stage.setOutputBytes(Long.parseLong(value));
				}
				else if ("outputRecords".equalsIgnoreCase(key)) {
					stage.setOutputRecords(Long.parseLong(value));
				}
				else if ("name".equalsIgnoreCase(key)) {
					stage.setName(value);
				}
				else if ("details".equalsIgnoreCase(key)) {
					stage.setDetails(value);
					lsStage.add(stage);
				}
			}
			//make header
			result.append("Id\tName\tStatus\tCompeleted Tasks\tFailed Tasks\tInputBytes/InputRecords\tOutputBytes/OutputRecords\tDetails");
			result.append("\n");
			// make result table body
			for (SparkMonitoringStage obj : lsStage) {
				result.append(obj.getId());
				result.append("\t").append(obj.getName());
				result.append("\t").append(obj.getStatus());
				int totalTasks = obj.getNumCompletedTasks() + obj.getNumFailedTasks();
				result.append("\t").append(obj.getNumCompletedTasks() + "/" + totalTasks);
				result.append("\t").append(obj.getNumFailedTasks() + "/" + totalTasks);
				//format 
				NumberFormat nf = new DecimalFormat(numberFormat);
				result.append("\t").append(nf.format(obj.getInputBytes() / 1000000D) + "MB/" + obj.getInputRecords());
				result.append("\t").append(nf.format(obj.getOutputBytes() / 1000000D) + "MB/" + obj.getOutputRecords());
				result.append("\t").append(obj.getDetails());
				result.append("\n"); // new row
			}

			return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
					result.toString());
		}

		return new InterpreterResult(InterpreterResult.Code.ERROR, "Error get data from server");
	}
	
	private InterpreterResult getJobInformation(
			List<String> lsItem,
			List<SparkMonitoringJob> lsJob) {
		// make return result - in TABLE type
		StringBuffer result = new StringBuffer();
		int lsItemLength = lsItem.size();
		// build the table header 
		int row = 0;
		for (int i = 0; i < lsItemLength; i++) {
			String key = lsItem.get(i);
			++i; //increase i to skip value field
			if ("jobId".equalsIgnoreCase(key)) {
				if (row == 0) {
					// add header in case row = 0
					result.append("Id");
				}
			} else if ("name".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tName");
				}
			} 
			else if ("submissionTime".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tSubmission Time");
				}
			} else if ("completionTime".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tCompletion Time");
				}
				// add duration
				if (row == 0) {
					result.append("\tDuration");
				}
			} else if ("status".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tStatus");
				}
			}
			else if ("numTasks".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tNum Tasks");
				}
			}
			else if ("numCompletedTasks".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tNum Completed");
				}
			}
			else if ("numFailedTasks".equalsIgnoreCase(key)) {
				if (row == 0) {
					result.append("\tNum Failed");
				}
				break;
			}
		}
		result.append("\n");
		// make result table body
		for (SparkMonitoringJob aJob : lsJob) {
			result.append(aJob.getId());
			result.append("\t").append(StringUtils.isNotEmpty(aJob.getDescription()) ? aJob.getDescription() + ": " : "").append(aJob.getName());
			result.append("\t").append(aJob.getSubmissionTime());
			result.append("\t").append(aJob.getCompletionTime());
			//calculate duration
			Long startTime = aJob.getSubmisstionTimeMilis();
			Long endTime = aJob.getCompletionTimeMilis();
			Double duration = (endTime - startTime) / 1000D;
			NumberFormat nf = new DecimalFormat(numberFormat);
			result.append("\t").append(nf.format(duration) + "s");
			result.append("\t").append(aJob.getStatus());
			result.append("\t").append(aJob.getNumTasks());
			result.append("\t").append(aJob.getNumCompletedTasks() + "/" + aJob.getNumTasks());
			result.append("\t").append(aJob.getNumFailedTasks() + "/" + aJob.getNumTasks());
			result.append("\n"); // new row
		}

		return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
				result.toString());
	}
	
	private InterpreterResult getJobStatistic(String[] urlItems, List<SparkMonitoringJob> lsJob) {
		StringBuffer result = new StringBuffer();
		String command = urlItems[1];
		//get statistic time range, if existed
		String time1 = null;
		String time2 = null;
		if(urlItems.length >= 3) {
			time1 = urlItems[2];
		}
		if(urlItems.length >= 4) {
			time2 = urlItems[3];
		}
		Long statStartTime = 0L;
		Long statEndTime = 0L;
		Calendar cal = Calendar.getInstance();
		if("hour".equalsIgnoreCase(command)) {
			//default is 00:00:00
			int startHour = 0;
			if(time1 != null) {
				try {
					startHour = Integer.parseInt(time1);
				} catch (NumberFormatException e) {
					startHour = 0;
				}
			}
			statStartTime = getTimeFromPattern(
					cal.get(Calendar.YEAR), 
					cal.get(Calendar.MONTH), 
					cal.get(Calendar.DATE), 
					startHour, 0, 0);
			
			//default is current hour
			int endHour = cal.get(Calendar.HOUR_OF_DAY);
			if(time2 != null) {
				try {
					endHour = Integer.parseInt(time2);
				} catch (NumberFormatException e) {
					//keep endHour
				}
			}
			statEndTime = getTimeFromPattern(
					cal.get(Calendar.YEAR), 
					cal.get(Calendar.MONTH), 
					cal.get(Calendar.DATE), 
					endHour, 0, 0);
			if(time2 == null) {
				//time1 is the period of start time before end time
				statStartTime = statEndTime - startHour * 3600 * 1000;
			}
		}
		else if("day".equalsIgnoreCase(command)) {
			//default is first day of month
			int startDay = 1;
			if(time1 != null) {
				try {
					startDay = Integer.parseInt(time1);
				} catch (NumberFormatException e) {
					startDay = 1;
				}
			}
			statStartTime = getTimeFromPattern(
					cal.get(Calendar.YEAR), 
					cal.get(Calendar.MONTH), 
					startDay, 
					0, 0, 0);
			
			//default is current date
			int endDay = cal.get(Calendar.DATE);
			if(time2 != null) {
				try {
					endDay = Integer.parseInt(time2);
				} catch (NumberFormatException e) {
					//keep endDay
				}
			}
			statEndTime = getTimeFromPattern(
					cal.get(Calendar.YEAR), 
					cal.get(Calendar.MONTH), 
					endDay, 
					23, 59, 59);
			if(time2 == null) {
				//time1 is the period of start time before end time
				statStartTime = statEndTime - startDay * 24 * 3600 * 1000;
			}
		}
		else if("month".equalsIgnoreCase(command)) {
			//default is first month of year
			int startMonth = Calendar.JANUARY;
			if(time1 != null) {
				try {
					startMonth = Integer.parseInt(time1);
				} catch (NumberFormatException e) {
					startMonth = Calendar.JANUARY;
				}
			}
			statStartTime = getTimeFromPattern(
					cal.get(Calendar.YEAR), 
					startMonth, 
					1, 
					0, 0, 0);
			
			//default is current month
			int endMonth = cal.get(Calendar.MONTH);
			if(time2 != null) {
				try {
					endMonth = Integer.parseInt(time2);
				} catch (NumberFormatException e) {
					//keep endMonth
				}
			}
			//to get the last day of current month
			//let the first time of next month
			statEndTime = getTimeFromPattern(
					cal.get(Calendar.YEAR), 
					endMonth + 1, 
					1, 
					0, 0, 0);
			//minus to 1 second
			statEndTime = statEndTime - 1 * 1000;
			
			if(time2 == null) {
				//time1 is the period of start time before end time
				statStartTime = statEndTime - startMonth * 30 * 24 * 3600 * 1000;
			}
		}
		else if("year".equalsIgnoreCase(command)) {
			//default is first year
			int startYear = 1;
			if(time1 != null) {
				try {
					startYear = Integer.parseInt(time1);
				} catch (NumberFormatException e) {
					startYear = 1;
				}
			}
			statStartTime = getTimeFromPattern(
					startYear, 
					1, 
					1, 
					0, 0, 0);
			
			//default is current year
			int endYear = cal.get(Calendar.YEAR);
			if(time2 != null) {
				try {
					endYear = Integer.parseInt(time2);
				} catch (NumberFormatException e) {
					//keep endYear
				}
			}
			statEndTime = getTimeFromPattern(
					endYear, 
					12, 
					31, 
					23, 59, 59);
			
			if(time2 == null) {
				//time1 is the period of start time before end time
				statStartTime = statEndTime - startYear * 365 * 24 * 3600 * 1000;
			}
		}
		// get statistic of number jobs by range of time
		// total_jobs, total_running, total_completed, total_failed
		int totalJob = 0;
		int totalRunning = 0;
		int totalCompleted = 0;
		int totalFailed = 0;
		for(SparkMonitoringJob job : lsJob) {
			Long startTime = job.getSubmisstionTimeMilis();
			if(startTime >= statStartTime && startTime <= statEndTime) {
				totalJob++;
				String jobStatus = job.getStatus();
				if("RUNNING".equals(jobStatus)) {
					totalRunning++;
				}
				else if("SUCCEEDED".equals(jobStatus)) {
					totalCompleted++;
				}
				else if("FAILED".equals(jobStatus)) {
					totalFailed++;
				}
			}
		}
		//add to result
		result.append("Total jobs\tRunning\tSucceeded\tFailed");
		result.append("\n");
		result.append(totalJob).append("\t").append(totalRunning);
		result.append("\t").append(totalCompleted).append("\t").append(totalFailed);
		
		return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
				result.toString());
	}
	
	private Long getTimeFromPattern(int year, int month, int date, int hourOfDay, int minute, int second) {
		Calendar cal = Calendar.getInstance();
		cal.set(year, month, date, hourOfDay, minute, second);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}
	
	private InterpreterResult getCpuUsage() {
		StringBuffer result = new StringBuffer();
		OperatingSystemMXBean osBean =
		         (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		
		
		return new InterpreterResult(InterpreterResult.Code.SUCCESS, InterpreterResult.Type.TABLE,
				result.toString());
	}

}
