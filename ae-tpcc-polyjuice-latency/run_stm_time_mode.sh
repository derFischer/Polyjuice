#!/bin/bash

exec=out-perf.masstree/benchmarks/dbtest
bench=seats
cores_per_numa=8
mem_per_numa=10

workload_mix=100,0,0,0,0
runtime=10

#THREADS=( 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60 62 )
THREADS=( 1 2 4 8 12 16 20 24 )
#THREADS_PER_WAREHOUSE=( 62 32 16 8 1 )

THREADS_PER_WAREHOUSE=( 62 )
n_threads=

bench_result="stm.csv"
rm -f ${bench_result}
touch ${bench_result}

threads_per_warehouse=

for threads_per_warehouse in ${THREADS_PER_WAREHOUSE[@]}
do
    scale_factor_str="scale_factor"
    threads_str="n_threads"
    throughput_sched_str="throughput"
    abort_rate_str="abort"
    a0_str="a0"
    a1_str="a1"
    a2_str="a2"
    a3_str="a3"
    a4_str="a4"
    a5_str="a5"
    a6_str="a6"
    a7_str="a7"
    a8_str="a8"
    a9_str="a9"
    for n_threads in ${THREADS[@]}
    do
        n_numas=$((${n_threads}/${cores_per_numa}))
        mod_buf=$((${n_threads}%${cores_per_numa}))
        if [ ${mod_buf} -ne 0 ]
        then
            n_numas=$((${n_numas}+1))
        fi
        numa_mem=$((${mem_per_numa}*${n_numas}))
        scale_factor=1
        if [ ${n_threads} -gt ${threads_per_warehouse} ]
        then
            scale_factor_check=$((${n_threads}/${threads_per_warehouse}))
            scale_factor_mul=$((${threads_per_warehouse}*${scale_factor_check}))
            if [ ${scale_factor_mul} -eq ${n_threads} ]
            then
                scale_factor=${scale_factor_check}
            else
                scale_factor=0
            fi
        fi
        #echo "numa_mem: ${numa_mem}"
        numa_mem="${numa_mem}G"
	echo "numa_mem: ${numa_mem}"
        if [ ${scale_factor} -ne 0 ]
        then
            scale_factor_str=${scale_factor_str}",${scale_factor}"
            threads_str=${threads_str}",${n_threads}"

            ntry=5
            ret=
            while [ ${ntry} -gt 0 ]
            do



                ret=`./${exec} \
                    --bench ${bench} \
                    --num-threads ${n_threads} \
                    --scale-factor ${scale_factor} \
                    --runtime ${runtime} \
                    --numa-memory ${numa_mem} \
                    | grep "RESULT" | sed "s/^RESULT \(.*\)$/\1/g"`
	
                if [ ${#ret} -ne 0 ]
                then
                    IFS=',' read throughput abort_rate a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 <<<"${ret}"
                    echo "success: throughput: ${throughput}, abort_rate: ${abort_rate}, a0: ${a0}, a1: ${a1}, a2: ${a2}, a3: ${a3}, a4: ${a4}, a5: ${a5}, a6: ${a6}, a7: ${a7}, a8: ${a8}, a9: ${a9}"
                    break
                else
                    ntry=$((${ntry}-1))
                fi
            done
	    #echo ${scale_factor}
            if [ ${#ret} -ne 0 ]
            then
                throughput_sched_str="${throughput_sched_str},${throughput}"
		abort_rate_str="${abort_rate_str},${abort_rate}"
                a0_str="${a0_str},${a0}"
                a1_str="${a1_str},${a1}"
                a2_str="${a2_str},${a2}"
                a3_str="${a3_str},${a3}"
                a4_str="${a4_str},${a4}"
                a5_str="${a5_str},${a5}"
                a6_str="${a6_str},${a6}"
                a7_str="${a7_str},${a7}"
                a8_str="${a8_str},${a8}"
                a9_str="${a9_str},${a9}"
            else
                throughput_sched_str="${throughput_sched_str},0"
		abort_rate_str="${abort_rate_str},0"
                a0_str="${a0_str},0"
                a1_str="${a1_str},0"
                a2_str="${a2_str},0"
                a3_str="${a3_str},0"
                a4_str="${a4_str},0"
                a5_str="${a5_str},0"
                a6_str="${a6_str},0"
                a7_str="${a7_str},0"
                a8_str="${a8_str},0"
                a9_str="${a9_str},0"
            fi
        fi
    done
    echo "CURRENT PROGRESS:"
    echo ${threads_str}
    echo ${throughput_sched_str}
    echo ${abort_rate_str}
    echo ${a0_str}
    echo ${a1_str}
    echo ${a2_str}
    echo ${a3_str}
    echo ${a4_str}
    echo ${a5_str}
    echo ${a6_str}
    echo ${a7_str}
    echo ${a8_str}
    echo ${a9_str}
    echo ${threads_per_warehouse} >>${bench_result}
    echo ${scale_factor_str} >>${bench_result}
    echo ${threads_str} >>${bench_result}
    echo ${throughput_sched_str} >>${bench_result}
    echo ${abort_rate_str} >> ${bench_result}
    echo ${a0_str} >> ${bench_result}
    echo ${a1_str} >> ${bench_result}
    echo ${a2_str} >> ${bench_result}
    echo ${a3_str} >> ${bench_result}
    echo ${a4_str} >> ${bench_result}
    echo ${a5_str} >> ${bench_result}
    echo ${a6_str} >> ${bench_result}
    echo ${a7_str} >> ${bench_result}
    echo ${a8_str} >> ${bench_result}
    echo ${a9_str} >> ${bench_result}
    echo "" >>${bench_result}
done
