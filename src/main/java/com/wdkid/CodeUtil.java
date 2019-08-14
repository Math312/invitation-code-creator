package com.wdkid;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.math.BigDecimal;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class CodeUtil {

    private final static int BULK_SIZE = 1024 * 8;

    private final static BigDecimal BULK_SIZE_BIGDICIMAL = BigDecimal.valueOf(1024).multiply(BigDecimal.valueOf(8));

    /* 邀请码最小长度 */
    private int MIN_LENGTH = 4;

    /* 邀请码中出现的字符 */
    private static final String[] CHAR_RESOURCE = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
            "U", "V", "W", "X", "Y", "Z"
    };

    /* redis桶的个数 */
    private int bulkLength;

    /* 指定长度的邀请码最多需要记录的数字的个数即CHAR_RESOURCE.length的length次方 */
    private BigDecimal total;

    /* 邀请码长度 */
    private int length;

    /* 保存邀请码的key的格式 */
    private String save_key_format;

    private JedisPool jedisPool;

    /* Lua脚本，用于读取并设置位 */
    private static final String script = "local index = redis.call('bitpos',KEYS[1],KEYS[2],KEYS[3]);\n" +
            "if index ~= -1 then\n" +
            "    local result = redis.call('setbit',KEYS[1],index,'0')\n" +
            "end\n" +
            "return index;";

    public CodeUtil(int length, String save_key_prefix) {
        if (length < MIN_LENGTH) {
            throw new RuntimeException();
        }
        this.save_key_format = save_key_prefix + "_%d";
        this.length = length;
        this.total = new BigDecimal(1);
        for (int i = 0; i < length; i++) {
            this.total = this.total.multiply(BigDecimal.valueOf(CHAR_RESOURCE.length));
        }
        this.bulkLength = this.total.divide(new BigDecimal(BULK_SIZE)).intValue();
    }

    /**
     * 初始化数据
     * */
    public void init(String addr, int port) {
        /*初始化Redis数据*/
        jedisPool = new JedisPool(addr, port);
        Jedis jedis = jedisPool.getResource();
        byte[] label = new byte[1024];
        for (int i = 0; i < label.length; i++) {
            label[i] = (byte) 0xFF;
        }
        for (int i = 0; i < this.bulkLength; i++) {
            String key = String.format(this.save_key_format, i);
            byte[] data = jedis.get(key.getBytes());
            if (data != null) {
                continue;
            }
            jedis.set(key.getBytes(), label);
        }
        jedis.close();
    }

    /**
     * 创建Code
     * */
    public String createCode() {
        Jedis jedis = jedisPool.getResource();
        while (true) {
            /* 先分桶，在获取随机数 */
            int bulkIndex = ThreadLocalRandom.current().nextInt(this.bulkLength);
            int offset = ThreadLocalRandom.current().nextInt(1024);
            System.out.println("bulkIndex:"+bulkIndex);
            System.out.println("offset:"+offset);
            String key = String.format(this.save_key_format,bulkIndex);
            Object result = jedis.eval(script,3,new String[]{key,"1",offset+""});
            if(!result.equals("-1")){
                String code = intToCode(bulkIndex,offset);
                jedis.close();
                return code;
            }
        }

    }

    /**
     * 数字转Code
     * */
    public String intToCode(int bulkIndex, int offset) {
        BigDecimal bigDecimal = new BigDecimal(bulkIndex).multiply(BULK_SIZE_BIGDICIMAL).add(BigDecimal.valueOf(offset));
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < this.length; i++) {
            BigDecimal[] result = bigDecimal.divideAndRemainder(new BigDecimal(36));
            int index = result[1].intValue();
            bigDecimal = result[0];
            stringBuilder.append(CHAR_RESOURCE[index]);
        }
        return stringBuilder.toString();
    }


    public static void main(String[] args) {
        CodeUtil codeUtil = new CodeUtil(4, "code");
        codeUtil.init("127.0.0.1", 6379);
        Scanner scanner = new Scanner(System.in);
        for (int i = 0;i < 5000;i ++){
            System.out.println("Please enter the new line:");
            scanner.nextLine();
            System.out.println(codeUtil.createCode());
        }
    }

}
