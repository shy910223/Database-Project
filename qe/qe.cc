
#include "qe.h"
#include  <stdio.h>
#include  <string.h>
#include <iostream>
using namespace std;

const RC success = 0;
const RC fail = -1;
const int LEN = 200; //// use a fixed length for now, need to be optimized.

Filter::Filter(Iterator* input, const Condition &condition) {

    input->getAttributes(attrs);

    void* in = malloc(LEN);
    void* out = malloc(LEN);

    while (input->getNextTuple(in) != QE_EOF) {
        void *record = malloc(LEN);
        retrieveRecord(in, attrs, condition.lhsAttr, out);
        if (compare(out, condition.rhsValue, condition.op, condition.rhsValue.type) == success) {
            memcpy(record, in, LEN);
            result.push_back((void*) record);
        }
    }
    this->pos = 0;

}

RC Filter::getNextTuple(void *data) {
    if (pos < result.size()) {
        memcpy(data, result[pos], LEN);
        pos++;
        return success;
    }
    pos = 0;
    return fail;

}

RC Filter::compare(void *data, Value value, CompOp op, AttrType attr) {

    if (value.type != attr)
        return fail; //two attribute type is different.
    else {
        int eq = 0;
        if (attr == TypeVarChar) {
            int charSize1 = 0, charSize2 = 0;
            char *val1, *val2;
            memcpy(&charSize1, (char*) data, sizeof (int));
            val1 = (char*) malloc(charSize1);

            memcpy(val1, (char*) data + sizeof (int), charSize1);
            memcpy(&charSize2, (char*) value.data, sizeof (int));

            val2 = (char*) malloc(charSize2);
            memcpy(val2, ((char*) value.data) + sizeof (int), charSize2);

            if (charSize1 != charSize2)
                return fail; // size two string is not equal
            else {
                eq = memcmp(val1, val2, charSize2);
                if (op == EQ_OP && eq == 0)
                    return success;
                if (op == NE_OP && eq != 0)
                    return success;
                if (op == NO_OP)
                    return success;
            }
            return fail;

        } else {
            if (attr == TypeInt) {

                int *lhs = (int*) malloc(sizeof (int));
                int *rhs = (int*) malloc(sizeof (int));

                memcpy(lhs, data, sizeof (int));
                memcpy(rhs, value.data, sizeof (int));
                switch (op) {
                    case EQ_OP:
                        if (*lhs == *rhs)
                            return success;
                        break;
                    case LT_OP:
                        if (*lhs < *rhs)
                            return success;
                        break;
                    case GT_OP:
                        if (*lhs > *rhs)
                            return success;
                        break;
                    case LE_OP:
                        if (*lhs <= *rhs)
                            return success;
                        break;
                    case GE_OP:
                        if (*lhs >= *rhs)
                            return success;
                        break;
                    case NE_OP:
                        if (*lhs != *rhs)
                            return success;
                        break;
                    case NO_OP:
                        return success;
                        break;
                    default:
                        break;
                }
            }
            if (attr == TypeReal) {
                float *lhs = (float*) malloc(sizeof (float));
                float *rhs = (float*) malloc(sizeof (float));

                memcpy(lhs, data, sizeof (float));
                memcpy(rhs, value.data, sizeof (float));

                switch (op) {
                    case EQ_OP:
                        if (*lhs == *rhs)
                            return 0;
                        break;
                    case LT_OP:
                        if (*lhs < *rhs)
                            return 0;
                        break;
                    case GT_OP:
                        if (*lhs > *rhs)
                            return 0;
                        break;
                    case LE_OP:
                        if (*lhs <= *rhs)
                            return 0;
                        break;
                    case GE_OP:
                        if (*lhs >= *rhs)
                            return 0;
                        break;
                    case NE_OP:
                        if (*lhs != *rhs)
                            return 0;
                        break;
                    case NO_OP:
                        return 0;
                        break;
                    default:
                        break;
                }

            }
        }
        return fail;
    }
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    for (int i = 0; i < this->attrs.size(); i++) {
        attrs.push_back(this->attrs[i]);
    }
}

void Iterator::retrieveRecord(void *data, vector<Attribute> &attrs, string attr, void *out) {

    int flag = 0, len = 0;

    for (unsigned i = 0; i < attrs.size(); i++) {
        if (strcmp(attr.c_str(), attrs[i].name.c_str()) == 0) {
            flag = true;
        }
        if (attrs[i].type == TypeReal) {
            if (flag == true) {
                memcpy(out, (char*) data + len, sizeof (float));

                break;
            }
            len += sizeof (float);
            continue;
        }
        if (attrs[i].type == TypeInt) {
            if (flag == true) {
                memcpy(out, (char*) data + len, sizeof (int));
                break;
            }
            len += sizeof (int);
            continue;

        }
        if (attrs[i].type == TypeVarChar) {
            int len2 = 0;
            memcpy(&len2, (char*) data + len, sizeof (int));

            if (flag == true) {
                memcpy(out, (char*) data + len, sizeof (int) +len2);
                break;
            }
            int string_length = len2 + sizeof (int);
            len += string_length;
        }

    }

}

Project::Project(Iterator *input, const vector<string> &attr) {
    this->iter = input;
    input->getAttributes(this->attrs);
    for (unsigned i = 0; i < attr.size(); i++) {
        
        for (unsigned j = 0; j < attrs.size(); j++) {
            if (attr[i] == attrs[j].name) {
                attribute.push_back(attrs[j]);
            }
        }

    }
}

RC Project::getNextTuple(void *data) {
    
    void *in = malloc(LEN);
    
    int offset = 0;
    if (iter->getNextTuple(in) != success)
        return QE_EOF;
    for (unsigned i = 0; i < attribute.size(); i++) {
        void *data_attr = malloc(LEN);
        retrieveRecord(in, attrs, attribute[i].name, data_attr);
        if (attribute[i].type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, data_attr, sizeof(int));
            memcpy((char*)data + offset, data_attr, sizeof(int) + strLen);
            offset += (sizeof(int) + strLen);
        } else {
            memcpy((char*)data + offset, data_attr, sizeof(int));
            offset += sizeof(int);
        }
    }
    return success;

}

void Project::getAttributes(vector<Attribute> &attrs) const {
    for (int i = 0; i < attribute.size(); i++) {
        attrs.push_back(attribute[i]);
    }
}

NLJoin::NLJoin(Iterator* leftIn, TableScan* rightIn, const Condition& condition, const unsigned numPages) {
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    leftAttr = condition.lhsAttr;
    rightAttr = condition.rhsAttr;
    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    leftData = malloc(1000);
    
    switch (condition.op) {
        case EQ_OP:
            compOp = EQ_OP;
            break;
        case LT_OP:
            compOp = GT_OP;
            break;
        case GT_OP:
            compOp = LT_OP;
            break;
        case LE_OP:
            compOp = GE_OP;
            break;
        case GE_OP:
            compOp = LE_OP;
            break;
        case NE_OP:
            compOp = NE_OP;
            break;
        case NO_OP:
            compOp = NO_OP;
            break;
        default:
            break;
    }
    // iterate to the end the rightIn
    void* data = malloc(1000);
    while (rightIn->getNextTuple(data) != QE_EOF);
    free(data);
}

NLJoin::~NLJoin() {
    rightIn->setIterator();
    free(leftData);
}

RC NLJoin::getNextTuple(void* data) {
    void* rightData = malloc(1000);
    while (rightIn->getNextTuple(rightData) == QE_EOF) {
        if (leftIn->getNextTuple(leftData) == QE_EOF) {
            free(rightData);
            return QE_EOF;
        }
        void* value = malloc(1000);
        
        retrieveRecord(leftData, leftAttrs, leftAttr, value);
        string attributeName = "";
        int i = 0;
        while (leftAttr[i] != '.') i++;
        i++;
        for (; i < leftAttr.size(); i++) {
            attributeName = attributeName + leftAttr[i];
        }
        rightIn->mySetIterator(attributeName, compOp, value);
        free(value);
    }
    int offset = 0;
    for (vector<Attribute>::iterator it = leftAttrs.begin(); it != leftAttrs.end(); it++) {
        if (it->type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char*)leftData + offset, sizeof(int));
            memcpy((char*)data + offset, (char*)leftData + offset, sizeof(int) + strLen);
            offset += (sizeof(int) + strLen);
        } else {
            memcpy((char*)data + offset, (char*)leftData + offset, sizeof(int));
            offset += sizeof(int);
        }
    }
    int r_offset = 0;
    for (vector<Attribute>::iterator it = rightAttrs.begin(); it != rightAttrs.end(); it++) {
        if (it->type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char*)rightData + r_offset, sizeof(int));
            memcpy((char*)data + offset, (char*)rightData + r_offset, sizeof(int) + strLen);
            offset += (sizeof(int) + strLen);
            r_offset += (sizeof(int) + strLen);
        } else {
            memcpy((char*)data + offset, (char*)rightData + r_offset, sizeof(int));
            offset += sizeof(int);
            r_offset += sizeof(int);
        }
    }
    free(rightData);
    return success;
}

void NLJoin::getAttributes(vector<Attribute>& attrs) const {
    for (int i = 0; i < leftAttrs.size(); i++) {
        attrs.push_back(leftAttrs[i]);
    }
    for (int i = 0; i < rightAttrs.size(); i++) {
        attrs.push_back(rightAttrs[i]);
    }
}

INLJoin::INLJoin(Iterator* leftIn, IndexScan* rightIn, const Condition& condition, const unsigned numPages) {
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    leftAttr = condition.lhsAttr;
    rightAttr = condition.rhsAttr;
    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    leftData = malloc(1000);
    
    switch (condition.op) {
        case EQ_OP:
            compOp = EQ_OP;
            break;
        case LT_OP:
            compOp = GT_OP;
            break;
        case GT_OP:
            compOp = LT_OP;
            break;
        case LE_OP:
            compOp = GE_OP;
            break;
        case GE_OP:
            compOp = LE_OP;
            break;
        case NE_OP:
            compOp = NE_OP;
            break;
        case NO_OP:
            compOp = NO_OP;
            break;
        default:
            break;
    }
    
    // iterate to the end the rightIn
    void* data = malloc(1000);
    while (rightIn->getNextTuple(data) != QE_EOF);
    free(data);
}

INLJoin::~INLJoin() {
    rightIn->setIterator(NULL, NULL, true, true);
    free(leftData);
}

RC INLJoin::getNextTuple(void* data) {
    void* rightData = malloc(1000);
    while (rightIn->getNextTuple(rightData) == QE_EOF) {
        if (leftIn->getNextTuple(leftData) == QE_EOF) {
            free(rightData);
            return QE_EOF;
        }
        void* value = malloc(1000);
        
        retrieveRecord(leftData, leftAttrs, leftAttr, value);
        string attributeName = "";
        int i = 0;
        while (leftAttr[i] != '.') i++;
        i++;
        for (; i < leftAttr.size(); i++) {
            attributeName = attributeName + leftAttr[i];
        }
        if (compOp == EQ_OP) rightIn->setIterator(value, value, true, true);
        if (compOp == LT_OP) rightIn->setIterator(NULL, value, true, false);
        if (compOp == GT_OP) rightIn->setIterator(value, NULL, false, true);
        if (compOp == LE_OP) rightIn->setIterator(NULL, value, true, true);
        if (compOp == GE_OP) rightIn->setIterator(value, NULL, true, true);
        if (compOp == NO_OP) rightIn->setIterator(NULL, NULL, false, false);
        free(value);
    }
    int offset = 0;
    for (vector<Attribute>::iterator it = leftAttrs.begin(); it != leftAttrs.end(); it++) {
        if (it->type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char*)leftData + offset, sizeof(int));
            memcpy((char*)data + offset, (char*)leftData + offset, sizeof(int) + strLen);
            offset += (sizeof(int) + strLen);
        } else {
            memcpy((char*)data + offset, (char*)leftData + offset, sizeof(int));
            offset += sizeof(int);
        }
    }
    int r_offset = 0;
    for (vector<Attribute>::iterator it = rightAttrs.begin(); it != rightAttrs.end(); it++) {
        if (it->type == TypeVarChar) {
            int strLen;
            memcpy(&strLen, (char*)rightData + r_offset, sizeof(int));
            memcpy((char*)data + offset, (char*)rightData + r_offset, sizeof(int) + strLen);
            offset += (sizeof(int) + strLen);
            r_offset += (sizeof(int) + strLen);
        } else {
            memcpy((char*)data + offset, (char*)rightData + r_offset, sizeof(int));
            offset += sizeof(int);
            r_offset += sizeof(int);
        }
    }
    free(rightData);
    return success;
}

void INLJoin::getAttributes(vector<Attribute>& attrs) const {
    for (int i = 0; i < leftAttrs.size(); i++) {
        attrs.push_back(leftAttrs[i]);
    }
    for (int i = 0; i < rightAttrs.size(); i++) {
        attrs.push_back(rightAttrs[i]);
    }
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
    this->input = input;
    this->aggAttr = aggAttr;
    this->op = op;
    input->getAttributes(attrs);
    int minInt, maxInt, sumInt;
    float minReal = 0, maxReal = 0, sumReal = 0;
    int count = 0;
    void* data = malloc(1000);
    void* aggData = malloc(1000);
    void* groupData = malloc(1000);
    
    bool flag = false;
    while (input->getNextTuple(data) != QE_EOF) {
        retrieveRecord(data, attrs, aggAttr.name, aggData);
        if (aggAttr.type == TypeInt) {
            int aggValue = *(int*)aggData;
            if (!flag) {
                minInt = aggValue;
                maxInt = aggValue;
                sumInt = aggValue;
                count = 1;
                flag = true;
                continue;
            }
            minInt = min(minInt, aggValue);
            maxInt = max(maxInt, aggValue);
            sumInt += aggValue;
            count += 1;
        } else {
            float aggValue = *(float*)aggData;
            if (!flag) {
                minReal = aggValue;
                maxReal = aggValue;
                sumReal = aggValue;
                count = 1;
                flag = true;
                continue;
            }
            minReal = min(minReal, aggValue);
            maxReal = max(maxReal, aggValue);
            sumReal += aggValue;
            count += 1;
        }
    }
    if (aggAttr.type == TypeInt) {
        if (op == MIN) aggIntValues.push_back(minInt);
        if (op == MAX) aggIntValues.push_back(maxInt);
        if (op == SUM) aggIntValues.push_back(sumInt);
        if (op == AVG) aggRealValues.push_back(((float)sumInt) / count);
        if (op == COUNT) aggIntValues.push_back(count);
    } else {
        if (op == MIN) aggRealValues.push_back(minReal);
        if (op == MAX) aggRealValues.push_back(maxReal);
        if (op == SUM) aggRealValues.push_back(sumReal);
        if (op == AVG) aggRealValues.push_back(sumReal / count);
        if (op == COUNT) aggRealValues.push_back(count);
    }
    
    pos = 0;
    free(data);
    free(aggData);
    free(groupData);
}

Aggregate::Aggregate(Iterator* input, Attribute aggAttr, Attribute gAttr, AggregateOp op) {
    this->input = input;
    this->aggAttr = aggAttr;
    this->op = op;
    input->getAttributes(attrs);
    map<int, int> minInt, maxInt, sumInt;
    map<int, float> minReal, maxReal, sumReal;
    map<int, int> count;
    void* data = malloc(1000);
    void* aggData = malloc(1000);
    void* groupData = malloc(1000);
    while (input->getNextTuple(data) != QE_EOF) {
        retrieveRecord(data, attrs, aggAttr.name, aggData);
        retrieveRecord(data, attrs, gAttr.name, groupData);
        
        int groupValue = *(int*)groupData;
        bool flag = false;
        for (int i = 0; i < groupValues.size(); i++) {
            if (groupValues[i] == groupValue) flag = true;
        }
        if (!flag) groupValues.push_back(groupValue);
        
        if (aggAttr.type == TypeInt) {
            int aggValue = *(int*)aggData;
            if (!flag) {
                minInt[groupValue] = aggValue;
                maxInt[groupValue] = aggValue;
                sumInt[groupValue] = aggValue;
                count[groupValue] = 1;
                continue;
            }
            minInt[groupValue] = min(minInt[groupValue], aggValue);
            maxInt[groupValue] = max(maxInt[groupValue], aggValue);
            sumInt[groupValue] += aggValue;
            count[groupValue] += 1;
        } else {
            float aggValue = *(float*)aggData;
            if (!flag) {
                minReal[groupValue] = aggValue;
                maxReal[groupValue] = aggValue;
                sumReal[groupValue] = aggValue;
                count[groupValue] = 1;
                continue;
            }
            minReal[groupValue] = min(minReal[groupValue], aggValue);
            maxReal[groupValue] = max(maxReal[groupValue], aggValue);
            sumReal[groupValue] += aggValue;
            count[groupValue] += 1;
        }
    }
    for (int i = 0; i < groupValues.size(); i++) {
        if (aggAttr.type == TypeInt) {
            if (op == MIN) aggIntValues.push_back(minInt[groupValues[i]]);
            if (op == MAX) aggIntValues.push_back(maxInt[groupValues[i]]);
            if (op == SUM) aggIntValues.push_back(sumInt[groupValues[i]]);
            if (op == AVG) aggRealValues.push_back(((float)sumInt[groupValues[i]]) / count[groupValues[i]]);
            if (op == COUNT) aggIntValues.push_back(count[groupValues[i]]);
        } else {
            if (op == MIN) aggRealValues.push_back(minReal[groupValues[i]]);
            if (op == MAX) aggRealValues.push_back(maxReal[groupValues[i]]);
            if (op == SUM) aggRealValues.push_back(sumReal[groupValues[i]]);
            if (op == AVG) aggRealValues.push_back(sumReal[groupValues[i]] / count[groupValues[i]]);
            if (op == COUNT) aggRealValues.push_back(count[groupValues[i]]);
        }
    }
    pos = 0;
    free(data);
    free(aggData);
    free(groupData);
}

Aggregate::~Aggregate() {
    
}

RC Aggregate::getNextTuple(void* data) {
    if (groupValues.size() == 0) {
        if (pos == 1) return QE_EOF;
        if (aggAttr.type == TypeInt && op != AVG)
            memcpy(data, &aggIntValues[0], sizeof(int));
        else
            memcpy(data, &aggRealValues[0], sizeof(int));
        pos++;
        return success;
    }
    if (pos == groupValues.size()) return QE_EOF;
    if (aggAttr.type == TypeInt && op != AVG) {
        memcpy(data, &groupValues[pos], sizeof(int));
        memcpy((char*)data + sizeof(int), &aggIntValues[pos], sizeof(int));
    } else {
        memcpy(data, &groupValues[pos], sizeof(int));
        memcpy((char*)data + sizeof(int), &aggRealValues[pos], sizeof(int));
    }
    pos++;
    return success;
}

void Aggregate::getAttributes(vector<Attribute>& attrs) const {
    for (int i = 0; i < this->attrs.size(); i++) {
        attrs.push_back(this->attrs[i]);
    }
}
