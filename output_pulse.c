/* 
 *  Squeezelite - lightweight headless squeezebox emulator
 *
 *  (c) Adrian Smith 2012-2015, triode1@btinternet.com
 *      Ralph Irving 2015-2021, ralph_irving@hotmail.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Additions (c) Paul Hermann, 2015-2021 under the same license terms
 *   -Control of Raspberry pi GPIO for amplifier power
 *   -Launch script on power status change from LMS
 */

// Output using PulseAudio

#include "squeezelite.h"

#if PULSEAUDIO

#include <pulse/pulseaudio.h>
#include <sys/mman.h>
#include <math.h>

// To report timing information back to the LMS the latency information needs to be
// retrieved from the PulseAudio server. However this eats some CPU cycles and a few
// bytes of RAM. Here you can decide if you want to retrieve precise timing information
// or save a few CPU cycles. If you are running squeezelite on resource constrained
// device (e.g. Raspberry Pi) and you want to keep the CPU temperature down then you
// can set the PULSEAUDIO_TIMING to zero. In this case the sound card latency and
// PulseAudio buffering won't be accounted for which might give you a slightly skewed
// playback timing information. Otherwise keep this to default value of 2 and you
// will get precise timing information.
#ifndef PULSEAUDIO_TIMING
#	define PULSEAUDIO_TIMING	2
#endif

typedef enum {
	readiness_unknown,
	readiness_ready,
	readiness_terminated,
} pulse_readiness;

typedef struct {
	pa_mainloop *loop;
	pa_context *ctx;
	pulse_readiness readiness;
} pulse_connection;

struct pulse {
	bool running;
	pa_stream *stream;
	pulse_readiness stream_readiness;
	pulse_connection conn;
	pa_sample_spec sample_spec;
	char *sink_name;
};

static struct pulse pulse;

static log_level loglevel;

extern struct outputstate output;
extern struct buffer *outputbuf;

#define OUTPUT_STATE_TIMER_INTERVAL_USEC   100000
#define PULSE_BUFFER_SIZE_MS 3000
#define PULSE_BUFFER_TARGET_MS 2250

#define LOCK   mutex_lock(outputbuf->mutex)
#define UNLOCK mutex_unlock(outputbuf->mutex)

extern u8_t *silencebuf;

static void pulse_state_cb(pa_context *c, void *userdata) {
	pa_context_state_t state;
	pulse_connection *conn = userdata;

	state = pa_context_get_state(c);
	switch  (state) {
		// There are just here for reference
		case PA_CONTEXT_UNCONNECTED:
		case PA_CONTEXT_CONNECTING:
		case PA_CONTEXT_AUTHORIZING:
		case PA_CONTEXT_SETTING_NAME:
		default:
			break;
		case PA_CONTEXT_FAILED:
		case PA_CONTEXT_TERMINATED:
			conn->readiness = readiness_terminated;
			break;
		case PA_CONTEXT_READY:
			conn->readiness = readiness_ready;
			break;
	}
}

static inline bool pulse_connection_is_ready(pulse_connection *conn) {
	return conn->readiness == readiness_ready;
}

static inline bool pulse_connection_check_ready(pulse_connection *conn) {
	if (!pulse_connection_is_ready(conn)) {
		LOG_ERROR("connection to PulseAudio server has been terminated");
		return false;
	}
	return true;
}

static inline void pulse_connection_iterate(pulse_connection *conn) {
	pa_mainloop_iterate(conn->loop, 1, NULL);
}

static inline pa_context * pulse_connection_get_context(pulse_connection *conn) {
	return conn->ctx;
}

#if PULSEAUDIO_TIMING > 0
static inline uint32_t pulse_get_usec_to_frames(pa_usec_t usec) {
	return (uint32_t)((usec * output.current_sample_rate) / PA_USEC_PER_SEC);
}
#endif

static inline uint32_t pulse_frames_to_usec(uint32_t frames) {
	return (uint32_t)((frames * PA_USEC_PER_SEC)/output.current_sample_rate);
}

static bool pulse_connection_init(pulse_connection *conn) {
	bool ret;
	
	conn->loop = pa_mainloop_new();
	pa_mainloop_api *api = pa_mainloop_get_api(conn->loop);
	pa_proplist *proplist = pa_proplist_new();
	pa_proplist_sets(proplist, PA_PROP_APPLICATION_VERSION, VERSION);
	conn->ctx = pa_context_new_with_proplist(api, MODEL_NAME_STRING, proplist);
	pa_proplist_free(proplist);

	conn->readiness = readiness_unknown;

	bool connected = false;

	if (pa_context_connect(conn->ctx, (const char *)NULL, PA_CONTEXT_NOFLAGS, (const pa_spawn_api *)NULL) < 0) {
		LOG_ERROR("failed to connect to PulseAudio server: %s", pa_strerror(pa_context_errno(conn->ctx)));
		ret = false;
	} else {
		connected = true;
		pa_context_set_state_callback(conn->ctx, pulse_state_cb, conn);
		while (conn->readiness == readiness_unknown) {
			pa_mainloop_iterate(conn->loop, 1, NULL);
		}

		ret = pulse_connection_is_ready(conn);
	}

	if (!ret) {
		if (connected) pa_context_disconnect(conn->ctx);
		pa_context_unref(conn->ctx);
		pa_mainloop_free(conn->loop);
	}

	return ret;
}

static void pulse_connection_destroy(pulse_connection *conn) {
	pa_context_disconnect(conn->ctx);
	pa_context_unref(conn->ctx);
	pa_mainloop_free(conn->loop);
}

static bool pulse_operation_wait(pulse_connection *conn, pa_operation *op) {
	if (op == NULL) {
		LOG_ERROR("PulseAudio operation failed: %s", pa_strerror(pa_context_errno(conn->ctx)));
		return false;
	}

	pa_operation_state_t op_state;
	while (pulse_connection_check_ready(conn) && (op_state = pa_operation_get_state(op)) == PA_OPERATION_RUNNING) {
		pulse_connection_iterate(conn);
	}

	pa_operation_unref(op);

	if (!pulse_connection_is_ready(conn))
		return false;

	return op_state == PA_OPERATION_DONE;
}

static void pulse_stream_state_cb(pa_stream *stream, void *userdata) {
	struct pulse *p = userdata;
	switch (pa_stream_get_state(stream)) {
		case PA_STREAM_UNCONNECTED:
		case PA_STREAM_CREATING:
			p->stream_readiness = readiness_unknown;
			break;
		case PA_STREAM_READY:
			p->stream_readiness = readiness_ready;
			break;
		case PA_STREAM_FAILED:
		case PA_STREAM_TERMINATED:
			p->stream_readiness = readiness_terminated;
			break;
	}
}

static void pulse_stream_success_noop_cb(pa_stream *s, int success, void *userdata) {
	LOG_DEBUG("OK");
}


static bool pulse_stream_create(struct pulse *p) {
	p->sample_spec.rate = output.current_sample_rate;
	p->sample_spec.format = PA_SAMPLE_S32LE; // SqueezeLite internally always uses this format, let PulseAudio deal with eventual resampling.
	p->sample_spec.channels = 2;

	pa_proplist *proplist = pa_proplist_new();
	pa_proplist_sets(proplist, PA_PROP_MEDIA_ROLE, "music");
	pa_proplist_sets(proplist, PA_PROP_MEDIA_SOFTWARE, "Logitech Media Server");

	p->stream = pa_stream_new_with_proplist(pulse_connection_get_context(&p->conn), "Logitech Media Server stream", &p->sample_spec, (const pa_channel_map *)NULL, proplist);
	pa_proplist_free(proplist);
	if (p->stream == NULL)
		return false;

	p->stream_readiness = readiness_unknown;
	pa_stream_set_state_callback(p->stream, pulse_stream_state_cb, p);

		//PA_STREAM_VARIABLE_RATE | PA_STREAM_AUTO_TIMING_UPDATE | PA_STREAM_INTERPOLATE_TIMING,
		//PA_STREAM_VARIABLE_RATE | PA_STREAM_AUTO_TIMING_UPDATE | PA_STREAM_INTERPOLATE_TIMING | PA_STREAM_ADJUST_LATENCY,
	if (pa_stream_connect_playback(p->stream, p->sink_name, (const pa_buffer_attr *)NULL,
#if PULSEAUDIO_TIMING == 2
		PA_STREAM_VARIABLE_RATE | PA_STREAM_AUTO_TIMING_UPDATE | PA_STREAM_ADJUST_LATENCY,
#else
		PA_STREAM_VARIABLE_RATE,
#endif
		(const pa_cvolume *)NULL, (pa_stream *)NULL) < 0) {
		pa_stream_unref(p->stream);
		p->stream = NULL;
		return false;
	}

	bool ok;
	while ((ok = pulse_connection_check_ready(&p->conn) && p->running) && p->stream_readiness == readiness_unknown) {
		pulse_connection_iterate(&p->conn);
	}

	ok = ok && p->stream_readiness == readiness_ready;

	if (ok) {
		pa_buffer_attr attr = { 0, };
		// Max pre adjust in lms is 3000ms allow for some addional tweeking
		attr.maxlength = BYTES_PER_FRAME * pulse_get_usec_to_frames(PULSE_BUFFER_SIZE_MS*1000u);
		attr.tlength = BYTES_PER_FRAME * pulse_get_usec_to_frames(PULSE_BUFFER_TARGET_MS*1000u);
		attr.prebuf = BYTES_PER_FRAME * pulse_get_usec_to_frames(PULSE_BUFFER_TARGET_MS*1000u);
		attr.minreq = (uint32_t)(-1);
		pa_operation *op = pa_stream_set_buffer_attr(p->stream, &attr, pulse_stream_success_noop_cb, NULL);
		ok = pulse_operation_wait(&p->conn, op);
	}

	if (!ok) {
		pa_stream_disconnect(p->stream);
		pa_stream_unref(p->stream);
		p->stream = NULL;
	}

	return ok;
}

static void pulse_stream_destroy(struct pulse *p) {
	if (p->stream) {
		pa_stream_disconnect(p->stream);
		pa_stream_unref(p->stream);
		p->stream = NULL;
	}
}

static void pulse_sinklist_cb(pa_context *c, const pa_sink_info *l, int eol, void *userdata) {
	if (eol == 0) {
		printf("  %-50s %s\n", l->name, l->description);
	} else if (eol < 0) {
		LOG_WARN("error while listing PulseAudio sinks");
	}
}

void list_devices(void) {
	pulse_connection conn;
	if (!pulse_connection_init(&conn))
		return;

	int state = 0;
	pa_operation *op;

	while (pulse_connection_check_ready(&conn)) {
		if (state == 0)	 {
			printf("Output devices:\n");
			op = pa_context_get_sink_info_list(pulse_connection_get_context(&conn), pulse_sinklist_cb, NULL);
			++state;
		} else if (pa_operation_get_state(op) == PA_OPERATION_DONE) {
			pa_operation_unref(op);
			break;
		}

		pulse_connection_iterate(&conn);
	}

	pulse_connection_destroy(&conn);
}

#if PULSEAUDIO_TIMING == 0
#	define DECLARE_LATENCY(n)			(void)0
#	define pulse_retrieve_latency(n,x)	true
#	define pulse_get_latency(n)			0
#	define pulse_device_frames			0
#elif PULSEAUDIO_TIMING == 2
#	define DECLARE_LATENCY(n)			pa_usec_t n
	static inline bool pulse_retrieve_latency(pa_usec_t *usec, int *neg) {
		return pa_stream_get_latency(pulse.stream, usec, neg) == 0;
	}
#endif


char *output_buffer_state_char(void) {
   char *c = "u";
   switch (output.state) {
    case OUTPUT_OFF:
      c = "o";
      break;
    case OUTPUT_STOPPED:
      c = "s";
      break;
    case OUTPUT_BUFFER:
      c = "b";
      break;
    case OUTPUT_RUNNING:
      c = "r";
      break;
    case OUTPUT_PAUSE_FRAMES:
      c = "p";
      break;
    case OUTPUT_SKIP_FRAMES:
      c = "s";
      break;
    case OUTPUT_START_AT:
      c = "a";
      break;
    default:
      break;
   }
   return c;
}

#define DEVICE_FRAME_INTERVAL_MS 900
#define DEVICE_FRAME_INIT_INTERRVAL_MS 2500

static uint32_t nextms = 0;
static uint32_t skiped = 0;

static uint32_t latency_max = 0;
static uint32_t latency_min = 0;

#define DEVICE_SKIP_AVERAGE_SIZE 15
static int skiped_pos = 0;
static int skiped_len = DEVICE_SKIP_AVERAGE_SIZE;
static uint64_t skiped_sum = 0;
static uint32_t skiped_avga[DEVICE_SKIP_AVERAGE_SIZE] = {0};

#define DEVICE_FRAME_AVERAGE_SIZE 5
static int latency_pos = 0;
static int latency_len = DEVICE_FRAME_AVERAGE_SIZE;
static uint64_t latency_sum = DEVICE_FRAME_AVERAGE_SIZE*PULSE_BUFFER_TARGET_MS*1000u;
//static uint32_t latency_avga[DEVICE_FRAME_AVERAGE_SIZE] = {PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u};
static uint32_t latency_avga[DEVICE_FRAME_AVERAGE_SIZE] = {PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u};
//static uint32_t latency_avga[DEVICE_FRAME_AVERAGE_SIZE] = {PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u};
//static uint32_t latency_avga[DEVICE_FRAME_AVERAGE_SIZE] = {PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u};
//static uint32_t latency_avga[DEVICE_FRAME_AVERAGE_SIZE] = {PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u,PULSE_BUFFER_TARGET_MS*1000u};
uint32_t uint32Avg(uint32_t *ptrArrNumbers,uint64_t *ptrSum, int *pos, int len, uint32_t nNum)
{
  //Subtract the oldest number from the prev sum, add the new number
  *ptrSum = *ptrSum - ptrArrNumbers[*pos] + nNum;
  //Assign the nextNum to the position in the array
  ptrArrNumbers[*pos] = nNum;
  //next pos
  *pos = *pos < len-1 ? *pos+1 :0;
  //return the average
  return (uint32_t) (*ptrSum / len);
}


static inline void pulse_device_frames(uint32_t timems) {
	DECLARE_LATENCY(latency_usec);
	int neg;

	if (output.state == OUTPUT_SKIP_FRAMES) {
	   skiped += output.skip_frames;
/*
	   //skiped = skiped > 0 ? (output.skip_frames+skiped/2) : output.skip_frames;
	   //skiped += output.skip_frames;
           if (pulse_frames_to_usec(output.skip_frames) > 45000u) {
		skiped += pulse_get_usec_to_frames(45000u);
	   } else {
	        skiped += pulse_get_usec_to_frames(1000u);
	        skiped += output.skip_frames;
	   }
	   skiped /=2;
	   LOG_DEBUG("PulseAudio skip %uf(%uus) skip total %uf(%ums)",(uint32_t)output.skip_frames,pulse_frames_to_usec(output.skip_frames),(uint32_t)  skiped ,pulse_frames_to_usec(skiped)/1000u);
	   */
	} else if (timems > nextms) {
	   bool lok = pulse_retrieve_latency(&latency_usec, &neg);
           uint32_t skiped_avg = uint32Avg(skiped_avga, &skiped_sum, &skiped_pos, skiped_len, pulse_frames_to_usec(skiped));
	   skiped = 0;
	   if (lok) {
	      frames_t device_frames = pulse_get_usec_to_frames(latency_usec)+1u;
	      uint32_t usec = pulse_frames_to_usec(output.device_frames);
	      int udiff = (int) latency_usec-usec;
              uint32_t latency_avg = uint32Avg(latency_avga, &latency_sum, &latency_pos, latency_len, latency_usec);

	      if ( device_frames > 1 ) {
/*
		 uint32_t ti = (timems - (nextms - DEVICE_FRAME_INTERVAL_MS));
                 //uint32_t tl = skiped / ti;
                 uint32_t tl = (skiped > 0) ? skiped : (skiped/2) ;

                 skiped_rolla = (skiped_rolla == 0 ) ? tl : (tl+skiped_rolla/2);
		 if (tl > skiped_latch) {
		    skiped_latch = tl;
	            LOG_DEBUG("PulseAudio set sl %uf",(uint32_t)skiped_latch);
		 } else {
	            if (skiped > 0) {
                       skiped_latch+= tl < ti ? skiped : tl;
	               LOG_DEBUG("PulseAudio add sl %ufsl %ufis %ums",(uint32_t)skiped_latch,(uint32_t)tl,(uint32_t)ti);
	            } else {
		       if (output.state == OUTPUT_RUNNING) {
		       //skiped_latch--;
		       }
		    }

*/
		 if ( latency_usec > latency_max) {
                     LOG_INFO("Max %u -> %uus",(uint32_t)latency_max,(uint32_t)latency_usec);
		     latency_max = latency_usec;
	         } else if ( latency_usec < latency_min ) {
                     LOG_INFO("Min %u -> %uus",(uint32_t)latency_min,(uint32_t)latency_usec);
                     latency_min = latency_usec;
		 }
	         LOG_DEBUG("l %s%s%uσ%+4d mam%u/%u/%ums %u",output_buffer_state_char(), (neg==1 ? "-":"+") ,(uint32_t)usec/1000u,(int)udiff/1000,(uint32_t) latency_min/1000u,latency_avg/1000u,latency_max/1000u,skiped_avg/1000u);
		 output.device_frames = 0;
		 output.device_frames += pulse_get_usec_to_frames(latency_avg)+1u;
		 //output.device_frames += device_frames;
	      } else {
                 LOG_WARN("PulseAudio Latency 0");
	      }
	      nextms = timems+DEVICE_FRAME_INTERVAL_MS;
	   } else {
              LOG_WARN("PulseAudio Latency failure");
	   }
	} else {
	      LOG_SDEBUG("Skip latency update %u %u, %uf",nextms,timems,output.device_frames);
	}
}

static void pulse_set_volume(struct pulse *p, unsigned left, unsigned right) {
	uint32_t sink_input_idx = pa_stream_get_index(p->stream);
	pa_cvolume volume;
	pa_cvolume_init(&volume);
	volume.channels = 2;
	volume.values[0] = pa_sw_volume_from_dB(20 * log10(left / 65536.0));
	volume.values[1] = left == right ? volume.values[0] : pa_sw_volume_from_dB(20 * log10(right / 65536.0));
	pa_operation *op = pa_context_set_sink_input_volume(pulse_connection_get_context(&p->conn), sink_input_idx, &volume, NULL, NULL);
	if (op != NULL) {
		// This is send and forget operation, dereference it right away.
		if (loglevel >= lDEBUG) {
			char s[20];
			LOG_DEBUG("sink input volume set to %s", pa_cvolume_snprint(s, sizeof(s), &volume));
		}
		pa_operation_unref(op);
	}
}

void set_volume(unsigned left, unsigned right) {
	bool adjust_sink_input = false;

	LOCK;
	adjust_sink_input = (left != output.gainL) || (right != output.gainR);
	output.gainL = left;
	output.gainR = right;
	UNLOCK;

	if (adjust_sink_input && pulse.stream != NULL) {
		pulse_set_volume(&pulse, left, right);
	}
}

void set_sample_rate(uint32_t sample_rate) {
	pa_operation *op = pa_stream_update_sample_rate(pulse.stream, sample_rate, NULL, NULL);
	if (op != NULL) {
		if (loglevel >= lDEBUG) {
			LOG_DEBUG("stream sample rate set to %d Hz", sample_rate);
		}
		pa_operation_unref(op);
	}
	else {
		LOG_WARN("failed to set stream sample rate to %d Hz", sample_rate);
	}
}

struct test_open_data {
	unsigned *rates;
	bool userdef_rates;
	pa_sample_spec *sample_spec;
	bool is_default_device;
	char *default_sink_name;
	bool got_device;
};

static void pulse_sinkinfo_cb(pa_context *c, const pa_sink_info *l, int eol, void *userdata) {
	if (eol) return;

	struct test_open_data *d = userdata;
	d->got_device = true;

	if (d->is_default_device)
		d->default_sink_name = strdup(l->name);

	if (!d->userdef_rates) {
		d->rates[0] = PA_RATE_MAX;
	}
	output.default_sample_rate = l->sample_spec.rate;

	*d->sample_spec = l->sample_spec;
}

bool test_open(const char *device, unsigned rates[], bool userdef_rates) {
	struct test_open_data d = {0, };
	d.rates = rates;
	d.userdef_rates = userdef_rates;
	d.sample_spec = &pulse.sample_spec;
	d.is_default_device = strcmp(device, "default") == 0;
	const char *sink_name = d.is_default_device ? NULL : device;
	LOG_DEBUG("Device: %s, Sink: %s",device,sink_name);
	pa_operation *op = pa_context_get_sink_info_by_name(pulse_connection_get_context(&pulse.conn), sink_name, pulse_sinkinfo_cb, &d);
	if (!pulse_operation_wait(&pulse.conn, op))
		return false;
	if (!d.got_device)
		return false;

	pulse.sink_name = d.is_default_device ? d.default_sink_name : (char *)device;

	return true;
}

static int _write_frames(frames_t out_frames, bool silence, s32_t gainL, s32_t gainR, u8_t flags,
						 s32_t cross_gain_in, s32_t cross_gain_out, s32_t **cross_ptr) {
	pa_stream_write(pulse.stream, silence ? silencebuf : outputbuf->readp, out_frames * BYTES_PER_FRAME, (pa_free_cb_t)NULL, 0, PA_SEEK_RELATIVE);
	return (int)out_frames;
}

void output_state_timer_cb(pa_mainloop_api *api, pa_time_event *e, const struct timeval *tv_, void *userdata) {
	struct pulse *p = userdata;
	pa_context_rttime_restart(pulse_connection_get_context(&p->conn), e, pa_rtclock_now() + OUTPUT_STATE_TIMER_INTERVAL_USEC);
}




static void * output_thread(void *arg) {
	pa_time_event *output_state_timer = NULL;
        output_state last = output.state;

	while (pulse.running) {
	        if (output.state == OUTPUT_OFF) {
			if (pulse.stream != NULL) {
				LOG_DEBUG("destroying PulseAudio playback stream");
				pulse_stream_destroy(&pulse);
			}
			
			if (output_state_timer == NULL) {
				LOG_DEBUG("PulseAudio output off timer");
				output_state_timer = pa_context_rttime_new(pulse_connection_get_context(&pulse.conn),
					pa_rtclock_now() + OUTPUT_STATE_TIMER_INTERVAL_USEC, output_state_timer_cb, &pulse);
			}
		} else {
			if (output_state_timer != NULL) {
				LOG_DEBUG("PulseAudio timer", pulse.sink_name);
				pa_mainloop_api *api = pa_mainloop_get_api(pulse.conn.loop);
				api->time_free(output_state_timer);
				output_state_timer = NULL;
			}
			
			if (pulse.stream == NULL) {
				if (pulse_stream_create(&pulse)) {
					LOG_DEBUG("PulseAudio playback stream on sink %s open", pulse.sink_name);
					unsigned left, right;
					LOG_INFO("Output Buffer %u",outputbuf->size);
					LOCK;
					latency_min = PULSE_BUFFER_TARGET_MS*1000u;
					latency_max = latency_min;
					output.device_frames = pulse_get_usec_to_frames(latency_min);
					output.start_frames = pulse_get_usec_to_frames(latency_min);
					left = output.gainL;
					right = output.gainR;
					output.updated = gettime_ms();
					nextms = output.updated + DEVICE_FRAME_INIT_INTERRVAL_MS;
					UNLOCK;
					pulse_set_volume(&pulse, left, right);
					LOG_INFO("Initial Device delay %uf %ums(%us)",output.device_frames,pulse_frames_to_usec(output.device_frames)/1000u,pulse_frames_to_usec(output.device_frames)/1000u/1000u);
				} else {
					if (!pulse.running)
						break;
					output.error_opening = true;
				}
			} else {
				size_t writable = pa_stream_writable_size(pulse.stream);
				if (writable > 0) {

					//DECLARE_LATENCY(latency_usec);
					//int neg;
					//bool latency_ok = pulse_retrieve_latency(&latency_usec, &neg);
					//frames_t frames_write = writable / pa_sample_size(pa_stream_get_sample_spec(pulse.stream));
					frames_t frames_write = writable / BYTES_PER_FRAME;
					//frames_t device_frames = pulse_get_latency_frames(latency_usec);

		                        LOCK;
/**
					if (latency_ok) {
						if ( device_frames != 0 ) {
						   output.device_frames = device_frames;
						   output.updated = gettime_ms();
						   output.frames_played_dmp = output.frames_played;
						} else {
						   if (output.device_frames != 0) { LOG_WARN("device frames was 0 currently %u",output.device_frames); }
						}
					}
**/
					// process frames
		                        //frames_t wrote = _output_frames(frame_write > device_frames*2 ? device_frames*2 : frame_write);
				        output.frames_played_dmp = output.frames_played;
					output.updated = gettime_ms();
					pulse_device_frames(output.updated);
		                        frames_t wrote = _output_frames(frames_write);
		                        UNLOCK;

			                //LOG_SDEBUG("latency:s d p w %c %s%ums%uf %u %u", buffer_state_char(), neg==1 ? "-":"+", latency_usec > 0 ? latency_usec / 1000:0, output.device_frames, output.frames_played, frames_write);

		                        if (!wrote) {
			                   LOG_SDEBUG("wrote 0 - sleeping");
			                   usleep(10000);
		                        }
				} else {
			               LOG_SDEBUG("writeable:no");
				       usleep(10000);
			        }
			}
		}

                last = output.state;
		pulse_connection_iterate(&pulse.conn);
	}

	pulse_stream_destroy(&pulse);
	
	return NULL;
}

static pthread_t thread;

void output_init_pulse(log_level level, const char *device, unsigned output_buf_size, char *params, unsigned rates[], unsigned rate_delay, unsigned idle, unsigned rt_priority) {
	loglevel = level;

	LOG_INFO("init output");

	output.format = 0;
	output.write_cb = &_write_frames;
	output.rate_delay = rate_delay;
	LOG_INFO("Output Buffer Size %u",output_buf_size);

	if (!pulse_connection_init(&pulse.conn)) {
		// In case of an error, the message is logged by the pulse_connection_init itself.
		exit(1);
	}

	output_init_common(level, device, output_buf_size, rates, idle);
	if (mlockall(MCL_CURRENT | MCL_FUTURE) == -1) {
                LOG_WARN("unable to lock memory: %s", strerror(errno));
        } else {
                LOG_INFO("memory locked");
        }

	// start output thread
	pulse.running = true;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setstacksize(&attr, PTHREAD_STACK_MIN + OUTPUT_THREAD_STACK_SIZE);
	pthread_create(&thread, &attr, output_thread, NULL);
	pthread_attr_destroy(&attr);

	// try to set this thread to real-time scheduler class, only works as root or if user has permission
	struct sched_param param;
	param.sched_priority = rt_priority;
	if (pthread_setschedparam(thread, SCHED_FIFO, &param) != 0) {
		LOG_WARN("unable to set output sched fifo: %s", strerror(errno));
	} else {
		LOG_INFO("set output sched fifo rt: %u", param.sched_priority);
	}
}

void output_close_pulse(void) {
	LOG_INFO("close output");

	pulse.running = false;
	pa_mainloop_wakeup(pulse.conn.loop);
	pthread_join(thread, NULL);

	if (output.device != pulse.sink_name)
		free(pulse.sink_name);

	pulse_connection_destroy(&pulse.conn);

	output_close_common();
}

void output_flush_pulse(void) {
	LOG_INFO("flush output");
	pa_stream_flush	(pulse.stream, NULL,NULL);
}
#endif
