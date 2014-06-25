#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

// aerospike includes.
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_val.h>
#include <aerospike/as_policy.h>

#define MAX_L 4096

// aerospike include ends.
typedef struct
{
	int n;
	char address[256][16];
	int port[256];
}ngx_http_as_hosts;

typedef struct
{
	aerospike *as;
	bool is_hosts_present;  // this is check if the hosts is same as prev. hosts
	ngx_str_t default_hosts;
	char default_namespace [40];
	ngx_http_as_hosts current_hosts; //store the current hosts to which as obj. is connected to
	bool connected;
	bool use_server_conf;
	
	ngx_pool_t *pool;
}ngx_http_as_conf_t;

typedef struct 
{
	char bin[1000];
	char value[1000];
	bool is_str;

}ngx_http_binvalue;


//static u_char connected[] = "Connected to aerospike!";
//static u_char not_connected[] = "Not connected to aerospike!";
/*static u_char connected_server[] = "Connected to server configuration!";
static u_char not_connected_server[] = "Could not connect to server configuration!";
static u_char connected_local[] = "Connected to local configuration!";
static u_char not_connected_local[] = "Could not connect to local configuration!";
static u_char not_entered[] = "Not entered the block";*/
//static u_char put_success[] = "Put successful";
//static u_char put_unsuccess[] = "Put not successful";



static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf);
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void* conf);
static char* ngx_http_as_use_srv_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_as_operate(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

bool ngx_http_as_operate_connect(ngx_http_request_t *r, ngx_http_as_conf_t *as_conf);
void ngx_http_as_utils_put(ngx_str_t url, aerospike *as, char response[]);
void ngx_http_as_operate_get(ngx_str_t url, aerospike *as, char response[]);
void ngx_http_as_operate_del(ngx_str_t url, aerospike *as, char response[]);

bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts);
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts);
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts);
bool ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char *arg, char value[]);
void ngx_http_as_utils_replace(char * o_string, char * s_string, char * r_string);
bool ngx_http_as_utils_compare_prev_new_hosts(ngx_http_as_hosts current_hosts, ngx_http_as_hosts hosts);
void ngx_http_as_utils_get_bin_value_pair(char b[], char v[],ngx_http_binvalue bv[], int size);

void ngx_http_as_utils_dump_error(as_error err, char response[], char* last_char);
void ngx_http_as_utils_dump_record(as_record *p_rec, as_error err, char response[]);
void ngx_http_as_utils_dump_bin(const as_bin* p_bin, char response[]);

static ngx_command_t ngx_http_as_commands[] = {
	{
		ngx_string("as_connect"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE1,
		ngx_http_as_connect,
		0,
		0,
		NULL
	},

	{
		ngx_string("as_use_srv_conf"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
		ngx_http_as_use_srv_conf,
		0,
		0,
		NULL
	},

	{
		ngx_string("as_operate"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
		ngx_http_as_operate,
		0,
		0,
		NULL
	},

	ngx_null_command
};

ngx_http_module_t ngx_http_as_module_ctx = {
	NULL,
	NULL,

	NULL,
	NULL,

	ngx_http_as_module_create_srv_conf,
	NULL,

	ngx_http_as_module_create_loc_conf,
	NULL
};

ngx_module_t ngx_http_as_module = {
	NGX_MODULE_V1,
	&ngx_http_as_module_ctx,
	ngx_http_as_commands,
	NGX_HTTP_MODULE,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NGX_MODULE_V1_PADDING
};

/* This function creates the server configuration of the aersopike moodules.
 * It allocates memory for the ngx_http_as_conf_t structre.
 */
static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf)
{

	// The configuration pointer to the structure.
	ngx_http_as_conf_t *conf;

	// Allocating memory using the default nginx function pcalloc.
	// It takes care of deallocating memory later.
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_conf_t));

	// Setting all the flag variables for the cluster objects to false.
	// No cluster objects initialised.
	conf->as = NULL;
	conf->connected = false;
	conf->use_server_conf = true;
	conf->pool = cf->pool;

	return conf;
}

/* This function creates the location configuration for the module.
 * It allocates memory for the nngx_http_as_loc_conf_t object for the location.
 */
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf)
{
	// creating a location object and allocating memory.
	ngx_http_as_conf_t *conf;
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_conf_t));

	// setting default values.
	conf->as = NULL;
	conf->connected = false;
	conf->use_server_conf = false;
	conf->pool = cf->pool;

	return conf;
}

static ngx_int_t ngx_http_as_operate_handler(ngx_http_request_t *r)
{

	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;
	
	rc = ngx_http_discard_request_body(r);

	if(rc!=NGX_OK)
		return rc;

	r->headers_out.content_type_len = sizeof("text/html")-1;
	r->headers_out.content_type.len = sizeof("text/html")-1;
	r->headers_out.content_type.data = (u_char *)"text/html";

	b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
	if(b==NULL)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	out.buf = b;
	out.next = NULL;

	ngx_http_as_conf_t *as_conf;
	as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);

	if(as_conf->use_server_conf)
		as_conf = ngx_http_get_module_srv_conf(r, ngx_http_as_module);

	bool is_connected = ngx_http_as_operate_connect(r, as_conf);
	char operation[20];
	ngx_http_as_utils_get_parsed_url_arguement(r->args, "op",operation);

	//ngx_write_stderr((char*)is_connected);
	if(is_connected && strcmp(operation,"put")==0)
	{
			char response[129000] = "\0";
			ngx_http_as_utils_put(r->args,as_conf->as,response);
			ngx_write_stderr(response);
			b->pos = (u_char *)response;
			b->last = (u_char *)response + sizeof(response) - 1;

			r->headers_out.status = NGX_HTTP_OK;
			r->headers_out.content_length_n = sizeof(response)-1;
		
	}
	else if(is_connected && strcmp(operation,"get")==0)
	{
		char response[129000] = "\0";
		ngx_http_as_operate_get(r->args, as_conf->as, response);
		ngx_write_stderr(response);
		b->pos = (u_char*)response;
		b->last = (u_char*)response + sizeof(response) - 1;

		r->headers_out.status = NGX_HTTP_OK;
		r->headers_out.content_length_n = sizeof(response)-1;
	}
	else if(is_connected && strcmp("del", operation)==0)
	{
		char response[129000] = "\0";
		ngx_http_as_operate_del(r->args, as_conf->as, response);
		
		b->pos = (u_char*)response;
		b->last = (u_char*)response + sizeof(response) - 1;

		r->headers_out.status = NGX_HTTP_OK;
		r->headers_out.content_length_n = sizeof(response)-1;

	}
	else if(!is_connected)
	{
		char response[129000] = "\0";
		as_error err_res;
		err_res.code = -1;
		strcpy(err_res.message,"AEROSPIKE_NOT_CONNECTED");
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err_res,response,NULL);
		b->pos = (u_char*)response;
		b->last = (u_char*)response + sizeof(response) - 1;

		r->headers_out.status = NGX_HTTP_OK;
		r->headers_out.content_length_n = sizeof(response)-1;
	}
	else
	{
		char response[129000] = "\0";
		as_error err_res;
		err_res.code = -1;
		strcpy(err_res.message,"AEROSPIKE_CONNECTED");
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err_res,response,NULL);
		b->pos = (u_char*)response;
		b->last = (u_char*)response + sizeof(response) - 1;

		r->headers_out.status = NGX_HTTP_OK;
		r->headers_out.content_length_n = sizeof(response)-1;
	}

	b->memory = 1;
	b->last_buf = 1;

	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
		return rc;

	return ngx_http_output_filter(r, &out);


}

bool ngx_http_as_operate_connect(ngx_http_request_t *r, ngx_http_as_conf_t *as_conf)
{
	//ngx_write_stderr("In ngx_http_as_operate_connect\n");
	//ngx_write_stderr((char*)r->args.data);



	// hosts stores the hosts address and ports to iniitailise the cluster object with.
	// hosts_arrived_in_url checks whether the request contains the ip and ports.
	ngx_http_as_hosts hosts;
	bool hosts_arrived_in_url = false;

	// getting the hosts string from the url.
	char hosts_string[1000] = "";
	ngx_http_as_utils_get_parsed_url_arguement(r->args, "hosts", hosts_string);


	// if the url contains ip and ports, setting the hosts_string to true, and parsing the host string.
	if(strlen(hosts_string)>0)
	{
		hosts_arrived_in_url = true;
		ngx_http_as_utils_get_hosts(hosts_string, &hosts);
		//if first tme connect then copy the host
		if(as_conf->current_hosts.n==0)
		{
			as_conf->current_hosts = hosts;
		}
		as_conf->is_hosts_present = ngx_http_as_utils_compare_prev_new_hosts(as_conf->current_hosts, hosts);
	}


	// if hosts arrived in url, we need to connect to that addresses.
	if(hosts_arrived_in_url && !(as_conf->is_hosts_present))
	{
		// if the object is previously connected, closing the connection.
		if(as_conf->connected)
		{
			as_error err;
			aerospike_close(as_conf->as, &err);
			aerospike_destroy(as_conf->as);
			as_conf->connected = false;
		}
			
		// connecting to new configurations.
		if(ngx_http_as_utils_connect(&(as_conf->as), hosts))
		{
			as_conf->connected = true;
			return true;
		}
	}
	else
	{
		// if using the default server hosts.
		// if the object is not yet connected, we need to initialize.
		if(!as_conf->connected)
		{
			ngx_http_as_utils_get_hosts((char*)as_conf->default_hosts.data, &hosts);
			if(as_conf->current_hosts.n==0)
			{
				as_conf->current_hosts = hosts;
			}
			if(ngx_http_as_utils_connect(&(as_conf->as), hosts))
			{
				as_conf->connected = true;
				return true;
			}
		}
		else
		{
			return true;
		}
	}
	return false;
}

/* This function sets up the as_connect directive.
 * It takes one arguements, which is the default hosts string, of the form, 127.0.0.1:3000,127.0.0.1:4000
 */
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	// Storing the arguemnts of the directive.
	ngx_str_t *arguments = cf->args->elts;

	// Acessing the server/local configuration.
	ngx_http_as_conf_t *as_conf;

	// Checking for the context, i.e. server/local.
	if(cf->cmd_type==NGX_HTTP_SRV_CONF)
		as_conf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_as_module);
	else
		as_conf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_as_module);

	// Stroing the default hosts provided in the arguement for the configuration.
	as_conf->default_hosts.data = arguments[1].data;
	as_conf->default_hosts.len = ngx_strlen(as_conf->default_hosts.data);

	return NGX_CONF_OK;
}

/* This funnction sets the flag for the local configuratation to use the server context object. */
static char* ngx_http_as_use_srv_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_as_conf_t *as_conf;
	as_conf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_as_module);
	as_conf->use_server_conf = true;
	return NGX_CONF_OK;
}

/* This function sets the handler for the as_operate_directive. */
static char* ngx_http_as_operate(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_core_loc_conf_t *clcf;
	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_as_operate_handler;
	return NGX_CONF_OK;
}

/* This function accepts an aerospike object, and the hosts to be connected.
 * It then created the connected to the cluster.
 * If the connection is succesful, it returns true, else false.
 */
bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts)
{
	// creating and initialising the as_config object.
	as_config cfg;
	as_config_init(&cfg);

	// adding the multiple ip and ports to the as_config object.
	ngx_http_as_utils_create_config(&cfg, hosts);

	// connecting to aerospike.
	*as = aerospike_new(&cfg);
	as_error err;
	if(aerospike_connect(*as, &err)!=AEROSPIKE_OK)
		return false;

	return true;
}

/* This function adds the different ip and ports, to the as_config object.*/
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts)
{
	int i;
	for(i=0; i<hosts.n; i++)
	{
		cfg->hosts[i].addr = hosts.address[i];
		cfg->hosts[i].port = hosts.port[i];
	}
}

/*This function takes as argument a character array of ip ports separeated by ";" which in turn are separated by ",".
 * It parses them into individual host ips and ports.
 */
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts)
{
	// temp_hosts stores the string combinations of host ip and port, merged using a semicolon.
	// for eg, 127.0.0.1:3000
	char temp_hosts[256][30];

	// pos stores the number of host ip and ports obtained. Set to 0.
	int pos = 0, i;

	// splitting up the different hosts, separated using ",".
	char *temp = strtok(arg, ",");
	while(temp!=NULL)
	{
		// copying the current host ip and port string into the temp_host array.
		strcpy(temp_hosts[pos], temp);
		pos++;

		temp = strtok(NULL, ",");
	}

	// setting the number of hosts as the value obtained above.
	hosts->n = pos;

	// For each host, separating the address and port.
	for(i=0; i<pos; i++)
	{
		temp = strtok(temp_hosts[i], ":");
		strcpy(hosts->address[i], temp);

		temp = strtok(NULL, ":");

		hosts->port[i] = atoi(temp);
	}
}


/*This function extracts the value of a field from the arguments provided
   in the url and puts it in the value[] and also returns true if 
   the value is string based on whether there is "" in the value
*/
bool ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char* arg, char value[])
{
	char temp2[1000], temp3[1000];
	char temp_args[100][1000];
	int pos = 0, i;
	bool flag = false;
	bool is_str = false;
	int len = url.len;
	char url_string[len];

	if(len>0)
	{
		char *temp = strtok((char*)url.data, " ");
		strcpy(url_string, temp);
		while(temp!=NULL)
			temp = strtok(NULL, " ");


		temp = strtok(url_string, "&");
		while(temp!=NULL)
		{
			strcpy(temp_args[pos], temp);
			pos++;

			temp = strtok(NULL, "&");
		}

		for(i=0; i<pos; i++)
		{
			temp = strtok(temp_args[i], "=");
			strcpy(temp2, temp);

			temp = strtok(NULL, "=");

			if(strcmp(temp2, arg)==0)
			{
				flag = 1;
				strcpy(temp3, temp);
				break;
			}
		}
		
		if(flag && (strcmp(temp2,"bin")!=0 && strcmp(temp2,"value")!=0))
		{
			is_str = (temp3[0]=='%')?true:false;
			//replace " i.e %22 with empty string
			ngx_http_as_utils_replace(temp3, "%22", "");
			
			strcpy(value, temp3);
		
		}
		else if(flag && (strcmp(temp2,"bin")==0 || strcmp(temp2,"value")==0))
			strcpy(value,temp3);
	}

	return is_str;
}

/* This function handles the put request. This fun. requires arguments url
from which it extracts everything like ns, set, key, bins, values. If multiple
bins and values is present then it checks if no. of bins and values are same or
not else return error. If multiple values are present then it uses structure binvalue
to combine bin-value pair for easy of access later. It then associate these with the 
key and calls "aerospike_put()".

*/


void ngx_http_as_utils_put(ngx_str_t url, aerospike *as, char response[])
{
	as_error err_res;
	if(as==NULL)
	{
		err_res.code = -1;
		strcpy(err_res.message,"AEROSPIKE_INSTANCE_NULL");
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err_res,response,NULL);
		return;
	}
	int i,countbin=0,countval=0;
	//extracts fields value from url
	char key[1000], namespace[40], set[100], bin[(int)url.len], value[(int)url.len];
	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);
	ngx_http_as_utils_get_parsed_url_arguement(url, "bin", bin);
	ngx_http_as_utils_get_parsed_url_arguement(url, "value", value);
	int len = strlen(bin);
	//count no. of bins and values
	for(i=0;i<len;i++)
	{
    	if((int)bin[i]==',')
        	countbin++;
	}
	len = strlen(value);
	for(i=0;i<len;i++)
	{
		if((int)value[i]==',')
			countval++;
	}
	//if not same then return error
	if(countbin!=countval)
	{
		//char response[129000] = "\0";
		err_res.code = -1;
		strcpy(err_res.message,"NUM_OF_BINS_AND_VALUES_MISMATCH");
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err_res,response,NULL);
		return;

	}
    
	ngx_http_binvalue binvalue[countbin+1];
	//put bin and value in the structure ngx_http_binvalue
	//for doing this it calls the below function
	ngx_http_as_utils_get_bin_value_pair(bin,value,binvalue,countbin+1);

	as_key put_key;
	as_key_init(&put_key, namespace, set, key);

	as_record rec;
	as_record_inita(&rec, countbin+1);
	for(i=0;i<countbin+1;i++)
	{

		if(binvalue[i].is_str)
		{
			strcpy(key,binvalue[i].bin);
			
			as_record_set_str(&rec, key, binvalue[i].value);
		}
		else
		{
			int value_int = atoi(binvalue[i].value);
			as_record_set_int64(&rec, binvalue[i].bin, value_int);
		}
	}

	as_error err;
	//calls aerospike_key_put and generate response based on the output of put
	if(aerospike_key_put(as, &err, NULL, &put_key, &rec)!=AEROSPIKE_OK)
	{
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err,response,NULL);
		return;
	}
	else
	{
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err,response,NULL);
		return ;
	}
}

/* This function handels the get request. It gets the ns, set and key
  from the url and then calls the aerospike_key_get().If error comes then
  it is send back as json containing the error msg. If there is not error
  then the response will contain the bins and values along with metadata 
  like TTL, generation etc. Mostly everything that is there in the record
  structure.
*/

void ngx_http_as_operate_get(ngx_str_t url, aerospike *as, char response[])
{
	as_error err_res;
	if(as==NULL)
	{
		err_res.code = -1;
		strcpy(err_res.message,"AEROSPIKE_INSTANCE_NULL");
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err_res,response,NULL);
		return;
	}

	char key[1000], namespace[40], set[100];
	//parses the arguments
	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);

	//initialize the key
	as_key get_key;
	as_key_init_str(&get_key, namespace, set, key);

	as_error err;
	as_record* p_rec = NULL;

	// Starting the json formatted string.
	strncat(response, "{\n", strlen("{\n"));

	// Read the (whole) test record from the database.
	if (aerospike_key_get(as, &err, NULL, &get_key, &p_rec) != AEROSPIKE_OK)
	{
		ngx_http_as_utils_dump_error(err, response, "");
	}
	else
	{
		//generate the response message containing everything described abve.
		ngx_http_as_utils_dump_error(err, response, ",");
		ngx_http_as_utils_dump_record(p_rec, err, response);
	}
}

/* This function handels the delete request. It gets the ns, set and key
  from the url and then calls the aerospike_key_remove().If error comes then
  it is send back as json containing the error msg. If there is not error
  then the response will contain Aerospike_ok msg.
*/
void ngx_http_as_operate_del(ngx_str_t url, aerospike *as, char response[])
{
	as_error err_res;
	if(as==NULL)
	{
		err_res.code = -1;
		strcpy(err_res.message,"AEROSPIKE_INSTANCE_NULL");
		strncat(response, "{\n", strlen("{\n"));
		ngx_http_as_utils_dump_error(err_res,response,NULL);
		return;
	}

	char key[1000], namespace[40], set[100];

	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);

	as_key del_key;
	as_key_init_str(&del_key, namespace, set, key);
	// Starting the json formatted string.
	strncat(response, "{\n", strlen("{\n"));

	as_error err;

	// Delete the (whole) test record from the database.
	aerospike_key_remove(as, &err, NULL, &del_key);
	
	ngx_http_as_utils_dump_error(err, response, "");
}

//Util function to replace to replace a character with a another char inplace
void ngx_http_as_utils_replace(char * o_string, char * s_string, char * r_string)
{
      //a buffer variable to do all replace things
      char buffer[MAX_L];
      //to store the pointer returned from strstr
      char * ch;
 
      //first exit condition
      if(!(ch = strstr(o_string, s_string)))
              return;
 
      //copy all the content to buffer before the first occurrence of the search string
      strncpy(buffer, o_string, ch-o_string);
 
      //prepare the buffer for appending by adding a null to the end of it
      buffer[ch-o_string] = 0;
 
      //append using sprintf function
      sprintf(buffer+(ch - o_string), "%s%s", r_string, ch + strlen(s_string));
 
      //empty o_string for copying
      o_string[0] = 0;
      strcpy(o_string, buffer);
      //pass recursively to replace other occurrences
      return ngx_http_as_utils_replace(o_string, s_string, r_string);
 }

/*This function check if the current hosts to whick client is connected is same as the new 
  one provided in the url. If they are same the we need not connect-an optimization
  returns true if same or false. Based on true or false we decide whethere we need to 
  connect again or not*/

 bool ngx_http_as_utils_compare_prev_new_hosts(ngx_http_as_hosts current_hosts, ngx_http_as_hosts hosts)
{

	/*
		current hosts ip:port pair is present in current_hosts and new ip:port is present in hosts
	*/
	if(current_hosts.n < hosts.n || current_hosts.n > hosts.n)
		return false;
	int i,j;
	bool flag = false;
	for(i=0;i<hosts.n;i++)
	{
		for(j=0;j<current_hosts.n;j++)
		{
			if((strcmp(current_hosts.address[j], hosts.address[i])==0))
			{
				if(current_hosts.port[j]==hosts.port[i])
					flag = true;
			}
		}
		//even if we find one ip:port which is not there in prev host then break and return false.
		if(!flag)
			break;
	}
	if(flag)
		return true;
	else
		return false;

}

/*
	Main job of this function is to extract the bin-value from the url 
	and put them in ngx_http_binvalue structure

*/

void ngx_http_as_utils_get_bin_value_pair(char b[], char v[],ngx_http_binvalue bv[], int size)
{
	int i=0;
	char *value,*bin;
	value = strtok(v,",");
	//extracts value first and put them in bv
	while(value)
	{
		bv[i].is_str = (*value =='%')?true:false;
		ngx_http_as_utils_replace(value, "%22", "");
		strcpy(bv[i].value, value);
		value = strtok(NULL,",");
		i++;
	}
	bin = strtok(b,",");
	i=0;
	//extracts bin first and put them in bv
	while(bin)
	{
		ngx_http_as_utils_replace(bin, "%22", "");
		strcpy(bv[i].bin, bin);
		bin = strtok(NULL,",");
		i++;
	}

}

/*This function generates the error or the reponse json 
which is then send to the client*/

void ngx_http_as_utils_dump_error(as_error err, char response[], char* last_char)
 {
 	//Starting the error block.
 	strncat(response, "\t\"Error\":\n\t{\n", strlen("\t\"Error\":\n\t{\n"));

 	// Adding the status code to the json string.
 	char status_code[5];
 	sprintf(status_code, "%d", err.code);
 	strncat(response, "\t\t\"Code\":", strlen("\t\t\"Code\":"));
 	strncat(response, status_code, strlen(status_code));
 	strncat(response, ",\n", strlen(",\n"));

 	// Adding the message to the json string.
 	strncat(response, "\t\t\"Message\":\"", strlen("\t\t\"Message\":\""));
 	strncat(response, err.message, strlen(err.message));
 	strncat(response, "\",\n", strlen("\",\n"));

 	// Adding the funtion where the error occured.
 	strncat(response, "\t\t\"Function\":\"", strlen("\t\t\"Function\":\""));
 	if(err.func==NULL)
 		strncat(response, "Null", strlen("Null"));
 	else
 		strncat(response, err.func, strlen(err.func));
 	strncat(response, "\",\n", strlen("\",\n"));

 	// Adding the file where the error occured.
 	strncat(response, "\t\t\"File\":\"", strlen("\t\t\"File\":\""));
 	if(err.func==NULL)
 		strncat(response, "Null", strlen("Null"));
 	else
 		strncat(response, err.file, strlen(err.file));
 	strncat(response, "\",\n", strlen("\",\n"));

 	// Adding the line where the error occured.
 	strncat(response, "\t\t\"Line\":", strlen("\t\t\"Line\":"));
 	if(err.func==NULL)
 		strncat(response, "\"Null\"", strlen("\"Null\""));
 	else
 	{
 		char line[10000] = "";
 		sprintf(line, "%d", (int)err.line);
 		strncat(response, line, strlen(line));
 	}
 	strncat(response, "\n", strlen("\n"));

 	// Ending the error block
 	if(last_char && strcmp(last_char, ",")==0)
 		strncat(response, "\t},\n", strlen("\t},\n"));
 	else
 		strncat(response, "\t}\n}", strlen("\t}\n}"));
 	ngx_write_stderr("IN the last fucntion\n");
 	ngx_write_stderr(response);
 }

 /* This function formats a bin as a json in the response string.
 * The first parameter is the bin to be formatted.
 * The second parameter is the response string.
 */
void ngx_http_as_utils_dump_bin(const as_bin* p_bin, char response[])
{
	// if the bin is null, writing to the log file.
 	if (! p_bin)
 	{
		ngx_write_stderr("Bin passed to dump_bin is null.\n");
		return;
	}

	// obtaing the value of bin as json formatted string.
	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	// writing the bin name, and the bin value into the response string.
	strncat(response, "\t\t", strlen("\t\t"));
	strncat(response, "\"", strlen("\""));
	strncat(response, as_bin_get_name(p_bin), strlen(as_bin_get_name(p_bin)));
	strncat(response, "\"", strlen("\""));
	strncat(response, ":", strlen(":"));
	strncat(response, val_as_str, strlen(val_as_str));

	free(val_as_str);
 }

 /* This function creates the json formatted string for a record.
 * The first parameter is the record, whose json formatting is to be done.
 * The second parameter is a character array, which stores the json of the record.
 */
void ngx_http_as_utils_dump_record(as_record *p_rec, as_error err, char response[])
{
	// If the record is null, write to the log file.
	if (! p_rec) {
		ngx_write_stderr("The record object passed to dump_record is null.");
		return;
	}

	// Obtaining the number of bins in the record.
	uint16_t num_bins = as_record_numbins(p_rec);

	// Starting metadata block.
	strncat(response, "\t\"Metadata\":\n\t{\n", strlen("\t\"Meta-data\":\n\t{\n"));

	// Appending the number of bins.
	char temp_json_string[100];
	sprintf(temp_json_string, "%d", (int)num_bins);
	strncat(response, "\t\t\"Num_bins\": ", strlen("\t\t\"Num_bins\": "));
	strncat(response, temp_json_string, strlen(temp_json_string));
	strncat(response, ",\n", strlen(",\n"));

	// Appending the generation of the record
	sprintf(temp_json_string, "%d", (int)p_rec->gen);
	strncat(response, "\t\t\"Generation\": ", strlen("\t\t\"Generation\": "));
	strncat(response, temp_json_string, strlen(temp_json_string));
	strncat(response, ",\n", strlen(",\n"));

	// appending the ttl.
	sprintf(temp_json_string, "%d", (int)p_rec->ttl);
	strncat(response, "\t\t\"Ttl\": ", strlen("\t\t\"Ttl\": "));
	strncat(response, temp_json_string, strlen(temp_json_string));
	strncat(response, "\n", strlen("\n"));

	// Ending metadata.
	strncat(response, "\t},\n", strlen("\t},\n"));

	// Starting the bins block
	strncat(response, "\t\"Bins\":\n\t{\n", strlen("\t\"Bins\":\n\t{\n"));
	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	// for json formatting, excluding the first bin.
	int ctr = 0;

	// for each bin in the record.
	while (as_record_iterator_has_next(&it))
	{

		// print "," after each record except the last one.
		if(ctr)
			strncat(response, ",\n", strlen(",\n"));

		// format the bin as json.
		ngx_http_as_utils_dump_bin(as_record_iterator_next(&it), response);
		ctr = 1;
	}

	// not appending "," at the end of last bin.
	strncat(response, "\n", strlen("\n"));

	as_record_iterator_destroy(&it);

	// Ending bin block
	strncat(response, "\t}\n", strlen("\t},\n"));

	// Ending string
	strncat(response, "}", strlen("}"));
 }