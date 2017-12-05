#pragma once

#include <iterator>
#include <array>
#include <bitset>
#include <intrin.h> 
#include <algorithm>
#include <vector>
#include <mutex>
#include <assert.h>

#ifndef TEST_STUFF
//#define TEST_STUFF
#endif

#ifdef TEST_STUFF 
#define Assert assert
#define IF_TEST_STUFF(x) x
#else
#define Assert(expression) ((void)0)
#define IF_TEST_STUFF(x)
#endif //TEST_STUFF

namespace MTObjects
{
	using std::vector;

	typedef unsigned short TChunkIndex;
	static const constexpr TChunkIndex kNullIndex = 0xFFFF;

#ifdef TEST_STUFF 
	struct TestStuff
	{
		static unsigned int& max_num_objects_to_handle() { static unsigned int value = 0; return value; }
		static unsigned int& max_num_data_chunks_used()  { static unsigned int value = 0; return value; }
		static unsigned int& max_num_clusters() { static unsigned int value = 0; return value; }
		static unsigned __int64& num_obj_cluster_overwritten() { static unsigned __int64 value = 0; return value; }
		static unsigned int& max_objects_to_merge() { static unsigned int value = 0; return value; }

		static void Reset()
		{
			max_num_objects_to_handle() = 0;
			max_num_data_chunks_used() = 0;
			max_num_clusters() = 0;
			num_obj_cluster_overwritten() = 0;
			max_objects_to_merge() = 0;
		}
	};
#endif //TEST_STUFF

	namespace SmartStackStuff
	{
		static const constexpr int kDataChunkSize = 64 * 4;
		struct DataChunk
		{
			static const constexpr int kStoragePerChunk = kDataChunkSize - (2 * sizeof(DataChunk*));

			DataChunk* previous_chunk_ = nullptr;
			DataChunk* next_chunk_ = nullptr;
			unsigned char memory_[kStoragePerChunk];

			void Clear()
			{
				previous_chunk_ = nullptr;
				next_chunk_ = nullptr;
				IF_TEST_STUFF(std::memset(memory_, 0xEEEE, kStoragePerChunk));
			}

			unsigned char* GetMemory()
			{
				return memory_;
			}
		};
		static_assert(sizeof(DataChunk) == kDataChunkSize);

		struct DataChunkMemoryPool64_Experimental
		{
			static const constexpr int kNumberChunks = 64 * 1024 - 2;

		private:
			std::array<DataChunk, kNumberChunks> chunks_;
			concurrency::concurrent_queue<TChunkIndex> unallocated_chunks;
			IF_TEST_STUFF(unsigned int num_chunks_allocated = 0);
		public:
			static DataChunkMemoryPool64_Experimental instance;

			DataChunkMemoryPool64_Experimental()
			{
				for (TChunkIndex i = 0; i < kNumberChunks; i++)
				{
					unallocated_chunks.push(i);
				}
			}
			~DataChunkMemoryPool64_Experimental() = default;
			DataChunkMemoryPool64_Experimental(DataChunkMemoryPool64_Experimental&) = delete;
			DataChunkMemoryPool64_Experimental& operator=(DataChunkMemoryPool64_Experimental&) = delete;

			bool AllFree() const
			{
				IF_TEST_STUFF(Assert(kNumberChunks - unallocated_chunks.unsafe_size() == num_chunks_allocated));
				return unallocated_chunks.unsafe_size() == kNumberChunks;
			}

			template<bool kThreadSafe> TChunkIndex Allocate()
			{
				TChunkIndex index = kNullIndex;
				const bool ok = unallocated_chunks.try_pop(index);
				Assert(ok);
				IF_TEST_STUFF(num_chunks_allocated++);
				IF_TEST_STUFF(TestStuff::max_num_data_chunks_used() = std::max<unsigned int>(TestStuff::max_num_data_chunks_used(), num_chunks_allocated));
				return index;
			}

			template<bool kThreadSafe> void Release(TChunkIndex index)
			{
				unallocated_chunks.push(index);
				IF_TEST_STUFF(num_chunks_allocated--);
			}

			DataChunk* GetChunk(TChunkIndex index)
			{
				return &chunks_[index];
			}

			TChunkIndex GetIndex(const DataChunk* chunk) const
			{
				return static_cast<TChunkIndex>(std::distance(&chunks_[0], chunk));
			}
		};

		struct DataChunkMemoryPool64
		{
			static const constexpr int kBitsetSize = 64; //size of range
			static const constexpr int kBitsetsInFirstLevel = 1;
			static const constexpr int kRangeNum = kBitsetSize * kBitsetsInFirstLevel;
			static const constexpr int kNumberChunks = kBitsetSize * kRangeNum;

			struct ExtendedBitset
			{
				static bool FirstZeroInBitset(const std::bitset<kBitsetSize>& bitset, unsigned long& out_index)
				{
					return 0 != _BitScanForward64(&out_index, ~bitset.to_ullong());
				}

				std::array<std::bitset<kBitsetSize>, kBitsetsInFirstLevel> bitsets_;

				unsigned int FirstZeroIndex() const
				{
					for (int i = 0; i < kBitsetsInFirstLevel; i++)
					{
						unsigned long result = 0;
						if (FirstZeroInBitset(bitsets_[i], result))
						{
							return result + kBitsetSize * i;
						}
					}

					Assert(false);

					return 0xFFFFFFFF;
				}

				bool Test(unsigned int i) const
				{
					return bitsets_[i / kBitsetSize][i % kBitsetSize];
				}

				void Set(unsigned int i, bool value)
				{
					bitsets_[i / kBitsetSize][i % kBitsetSize] = value;
				}
			};

		private:
			std::array<DataChunk, kNumberChunks> chunks_;
			std::array<std::bitset<kBitsetSize>, kRangeNum> is_element_occupied_;
			ExtendedBitset is_range_fully_occupied_;
			IF_TEST_STUFF(unsigned int num_chunks_allocated = 0);
			std::mutex mutex_;
		public:
			static DataChunkMemoryPool64 instance;

			DataChunkMemoryPool64() = default;
			~DataChunkMemoryPool64() = default;
			DataChunkMemoryPool64(DataChunkMemoryPool64&) = delete;
			DataChunkMemoryPool64& operator=(DataChunkMemoryPool64&) = delete;

			bool AllFree() const
			{
				for (auto& bs : is_element_occupied_)
				{
					if (bs.any())
						return false;
				}
				IF_TEST_STUFF(Assert(0 == num_chunks_allocated));
				return true;
			}

			static TChunkIndex FirstZeroInBitset(const std::bitset<kBitsetSize>& bitset)
			{
				unsigned long result = 0xFFFFFFFF;
				const bool ok = ExtendedBitset::FirstZeroInBitset(bitset, result);
				Assert(ok);
				return static_cast<TChunkIndex>(result);
			}

			template<bool kThreadSafe> TChunkIndex Allocate()
			{
				auto implementation = [&]()
				{
					IF_TEST_STUFF(Assert(num_chunks_allocated < kNumberChunks));

					const auto first_range_with_free_space = is_range_fully_occupied_.FirstZeroIndex();
					Assert(first_range_with_free_space < kRangeNum);
					auto& range_bitset = is_element_occupied_[first_range_with_free_space];

					Assert(!range_bitset.all());
					const auto bit_idx = FirstZeroInBitset(range_bitset);

					Assert(!range_bitset[bit_idx]);
					range_bitset[bit_idx] = true;

					is_range_fully_occupied_.Set(first_range_with_free_space, range_bitset.all());

					IF_TEST_STUFF(num_chunks_allocated++);
					IF_TEST_STUFF(TestStuff::max_num_data_chunks_used() = std::max<unsigned int>(TestStuff::max_num_data_chunks_used(), num_chunks_allocated));

					auto chunk_index = first_range_with_free_space * kBitsetSize + bit_idx;
					Assert(chunk_index != kNullIndex);
					return static_cast<TChunkIndex>(chunk_index);
				};

				if constexpr(kThreadSafe)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					return implementation();
				}
				else
				{
					return implementation();
				}
			}

			template<bool kThreadSafe> void Release(TChunkIndex index)
			{
				auto implementation = [&]()
				{
					Assert(index >= 0 && index < kNumberChunks);

					const auto range = index / kBitsetSize;
					const auto bit_idx = index % kBitsetSize;

					auto& range_bitset = is_element_occupied_[range];
					Assert(range_bitset[bit_idx]);
					range_bitset[bit_idx] = false;

					is_range_fully_occupied_.Set(range, false);
					IF_TEST_STUFF(num_chunks_allocated--);
				};

				if constexpr(kThreadSafe)
				{
					std::lock_guard<std::mutex> lock(mutex_);
					implementation();
				}
				else
				{
					implementation();
				}
			}

			DataChunk* GetChunk(TChunkIndex index)
			{
				return &chunks_[index];
			}

			TChunkIndex GetIndex(const DataChunk* chunk) const
			{
				return static_cast<TChunkIndex>(std::distance(&chunks_[0], chunk));
			}

		};
	};

	/*
	SmartStack is optimized for:
	- push_back, back, pop_back
	- unordered merge
	*/
	template<typename T>
	struct SmartStack
	{
		static_assert(SmartStackStuff::DataChunk::kStoragePerChunk >= sizeof(T), "too big T");
		static const constexpr unsigned int kElementsPerChunk = SmartStackStuff::DataChunk::kStoragePerChunk / sizeof(T);

		TChunkIndex first_chunk_ = kNullIndex;
		TChunkIndex last_chunk_ = kNullIndex;
		unsigned short number_chunks_ = 0;
		unsigned short number_of_elements_in_last_chunk_ = kElementsPerChunk;

	private:
		static SmartStackStuff::DataChunk* GetPtr(TChunkIndex index)
		{
			return (kNullIndex != index) ? SmartStackStuff::DataChunkMemoryPool64::instance.GetChunk(index) : nullptr;
		}

		static TChunkIndex GetIndex(SmartStackStuff::DataChunk* chunk)
		{
			return (nullptr != chunk) ? SmartStackStuff::DataChunkMemoryPool64::instance.GetIndex(chunk) : kNullIndex;
		}

		template<bool kThreadSafe> void AllocateNextChunk()
		{
			auto new_chunk = SmartStackStuff::DataChunkMemoryPool64::instance.Allocate<kThreadSafe>();
			Assert(new_chunk != kNullIndex);
			auto new_chunk_ptr = GetPtr(new_chunk);
			new_chunk_ptr->Clear();

			number_chunks_++;
			Assert(kElementsPerChunk == number_of_elements_in_last_chunk_);
			number_of_elements_in_last_chunk_ = 0;

			if (kNullIndex != last_chunk_)
			{
				auto last_chunk_ptr = GetPtr(last_chunk_);
				Assert(nullptr == last_chunk_ptr->next_chunk_);
				last_chunk_ptr->next_chunk_ = new_chunk_ptr;
				new_chunk_ptr->previous_chunk_ = last_chunk_ptr;
				last_chunk_ = new_chunk;
			}
			else
			{
				Assert(kNullIndex == first_chunk_);
				last_chunk_ = first_chunk_ = new_chunk;
			}
		}

		template<bool kThreadSafe> void ReleaseLastChunk()
		{
			auto chunk_to_release = last_chunk_;
			Assert(kNullIndex != chunk_to_release);

			number_chunks_--;
			Assert(0 == number_of_elements_in_last_chunk_);
			number_of_elements_in_last_chunk_ = kElementsPerChunk;
			SmartStackStuff::DataChunkMemoryPool64::instance.Release<kThreadSafe>(chunk_to_release);

			last_chunk_ = GetIndex(GetPtr(chunk_to_release)->previous_chunk_);
			if (kNullIndex != last_chunk_)
			{
				GetPtr(last_chunk_)->next_chunk_ = nullptr;
			}
			else
			{
				Assert(chunk_to_release == first_chunk_);
				Assert(0 == number_chunks_);
				first_chunk_ = kNullIndex;
			}
		}

		T* ElementsInLastChunk() const
		{
			Assert(kNullIndex != last_chunk_);
			return reinterpret_cast<T*>(SmartStackStuff::DataChunkMemoryPool64::instance.GetChunk(last_chunk_)->GetMemory());
		}
	public:
		unsigned int size() const
		{
			return ((int)number_chunks_ - 1) * kElementsPerChunk + number_of_elements_in_last_chunk_;
		}

		bool empty() const
		{
			return 0 == size();
		}

		T& back()
		{
			return ElementsInLastChunk()[number_of_elements_in_last_chunk_ - 1];
		}

		const T& back() const
		{

			return ElementsInLastChunk()[number_of_elements_in_last_chunk_ - 1];
		}

		template<bool kThreadSafe> void push_back(const T& value)
		{
			if (kElementsPerChunk == number_of_elements_in_last_chunk_)
			{
				AllocateNextChunk<kThreadSafe>();
			}
			new (ElementsInLastChunk() + number_of_elements_in_last_chunk_) T(value);
			number_of_elements_in_last_chunk_++;
		}

		template<bool kThreadSafe> void pop_back()
		{
			Assert(!empty());
			number_of_elements_in_last_chunk_--;
			(ElementsInLastChunk() + number_of_elements_in_last_chunk_)->~T();
			IF_TEST_STUFF(std::memset((ElementsInLastChunk() + number_of_elements_in_last_chunk_), 0xEEEE, sizeof(T)));
			if (0 == number_of_elements_in_last_chunk_)
			{
				ReleaseLastChunk<kThreadSafe>();
			}
		}

		template<bool kThreadSafe> void clear()
		{
			static_assert(std::is_pod<T>::value);
			if constexpr (std::is_pod<T>::value)
			{
				IF_TEST_STUFF(int released_chunks = 0);
				for (auto chunk_ptr = GetPtr(first_chunk_); chunk_ptr;)
				{
					auto temo_ptr = chunk_ptr;
					chunk_ptr = chunk_ptr->next_chunk_;
					Assert(nullptr == chunk_ptr || (chunk_ptr->previous_chunk_ == temo_ptr));
					SmartStackStuff::DataChunkMemoryPool64::instance.Release<kThreadSafe>(GetIndex(temo_ptr));
					IF_TEST_STUFF(released_chunks++);
				}
				IF_TEST_STUFF(Assert(number_chunks_ == released_chunks));
				first_chunk_ = kNullIndex;
				last_chunk_ = kNullIndex;
				number_chunks_ = 0;
				number_of_elements_in_last_chunk_ = kElementsPerChunk;
			}
			else
			{
				while (number_chunks_)
				{
					pop_back<kThreadSafe>();
				}
				Assert(first_chunk_ == kNullIndex);
				Assert(last_chunk_ == kNullIndex);
				Assert(number_chunks_ == 0);
				Assert(number_of_elements_in_last_chunk_ == kElementsPerChunk);
			}
		}

	public:
		struct Iter : public std::iterator<std::bidirectional_iterator_tag, T>
		{
		private:
			SmartStackStuff::DataChunk* chunk_ = nullptr;
			int element_index_ = 0;

			T* GetChunkElements() const
			{
				Assert(chunk_);
				return reinterpret_cast<T*>(chunk_->GetMemory());
			}

			void Increment()
			{
				Assert(chunk_);
				if ((SmartStack::kElementsPerChunk - 1) == element_index_)
				{
					chunk_ = chunk_->next_chunk_;
					element_index_ = 0;
				}
				else
				{
					element_index_++;
					Assert(element_index_ >= 0 && element_index_ < SmartStack::kElementsPerChunk);
				}
			}

			void Decrement()
			{
				Assert(chunk_);
				if (0 == element_index_)
				{
					chunk_ = chunk_->previous_chunk_;
					Assert(chunk_);
					element_index_ = SmartStack::kElementsPerChunk - 1;
				}
				else
				{
					element_index_--;
					Assert(element_index_ >= 0 && element_index_ < SmartStack::kElementsPerChunk);
				}
			}
		public:
			Iter() = default;
			Iter(SmartStackStuff::DataChunk* in_chunk, int in_element_index)
				: chunk_(in_chunk), element_index_(in_element_index)
			{
				Assert(chunk_ || !element_index_);
				Assert(element_index_ >= 0 && element_index_ < SmartStack::kElementsPerChunk);
			}
			Iter(const Iter& other) = default;
			Iter& operator=(const Iter& other) = default;

			Iter& operator++()
			{
				Increment();
				return *this;
			}

			Iter operator++(int)
			{
				Iter result(*this);
				Increment();
				return result;
			}

			Iter& operator--()
			{
				Decrement();
				return *this;
			}

			Iter operator--(int)
			{
				Iter result(*this);
				Decrement();
				return result;
			}

			T& operator*() const
			{
				return GetChunkElements()[element_index_];
			}

			T& operator->() const
			{
				return GetChunkElements()[element_index_];
			}

			bool operator ==(const Iter& other) const
			{
				return chunk_ == other.chunk_
					&& element_index_ == other.element_index_;
			}

			bool operator !=(const Iter& other) const
			{
				return chunk_ != other.chunk_
					|| element_index_ != other.element_index_;
			}
		};

		Iter begin() const
		{
			return Iter(GetPtr(first_chunk_), 0);
		}

		Iter end() const
		{
			Assert(!empty() || first_chunk_ == kNullIndex);
			Assert(!empty() || last_chunk_ == kNullIndex);

			if (empty() || (number_of_elements_in_last_chunk_ == SmartStack::kElementsPerChunk))
			{
				return Iter(nullptr, 0);
			}
			return Iter(GetPtr(last_chunk_), number_of_elements_in_last_chunk_);
		}

	public:
		SmartStack() = default;
		~SmartStack()
		{
			clear<false>();
		}

		SmartStack(SmartStack&& other)
		{
			first_chunk_ = other.first_chunk_;
			last_chunk_ = other.last_chunk_;
			number_chunks_ = other.number_chunks_;
			number_of_elements_in_last_chunk_ = other.number_of_elements_in_last_chunk_;

			other.first_chunk_ = kNullIndex;
			other.last_chunk_ = kNullIndex;
			other.number_chunks_ = 0;
			other.number_of_elements_in_last_chunk_ = kElementsPerChunk;
		}

		SmartStack& operator=(SmartStack&& other)
		{
			if (this != &other)
			{
				clear();

				first_chunk_ = other.first_chunk_;
				last_chunk_ = other.last_chunk_;
				number_chunks_ = other.number_chunks_;
				number_of_elements_in_last_chunk_ = other.number_of_elements_in_last_chunk_;

				other.first_chunk_ = kNullIndex;
				other.last_chunk_ = kNullIndex;
				other.number_chunks_ = 0;
				other.number_of_elements_in_last_chunk_ = kElementsPerChunk;
			}
			return *this;
		}
#ifdef TEST_STUFF 
		void ValidateNumberOfChunks() const
		{
			int counter = 0;
			for (auto chunk_ptr = GetPtr(first_chunk_); chunk_ptr; chunk_ptr = chunk_ptr->next_chunk_)
			{
				counter++;
			}
			Assert(number_chunks_ == counter);
		}
#endif
		template<bool kThreadSafe> static void UnorderedMerge(SmartStack& dst, SmartStack& src)
		{
			IF_TEST_STUFF(src.ValidateNumberOfChunks());
			IF_TEST_STUFF(dst.ValidateNumberOfChunks());

			if (src.empty()) { return; }
			if (dst.empty())
			{
				Assert(kNullIndex == dst.first_chunk_ && 0 == dst.number_chunks_);
				// Move everything
				dst.first_chunk_ = src.first_chunk_;
				dst.last_chunk_ = src.last_chunk_;
				dst.number_chunks_ = src.number_chunks_;
				dst.number_of_elements_in_last_chunk_ = src.number_of_elements_in_last_chunk_;
			}
			else
			{
				// Move some elements from last chunk
				const unsigned short num_free_slots_in_last_chunk_dst = kElementsPerChunk - dst.number_of_elements_in_last_chunk_;
				const auto num_elements_to_move = std::min(num_free_slots_in_last_chunk_dst, src.number_of_elements_in_last_chunk_);
				static_assert(std::is_pod<T>::value);
				if constexpr(std::is_pod<T>::value)
				{
					if (num_elements_to_move)
					{
						const unsigned int remaining_elements_in_last_chunk_src = src.number_of_elements_in_last_chunk_ - num_elements_to_move;
						std::memcpy(&dst.ElementsInLastChunk()[dst.number_of_elements_in_last_chunk_]
							, &src.ElementsInLastChunk()[remaining_elements_in_last_chunk_src]
							, num_elements_to_move * sizeof(T));
						dst.number_of_elements_in_last_chunk_ += num_elements_to_move;
						src.number_of_elements_in_last_chunk_ -= num_elements_to_move;
						if (0 == src.number_of_elements_in_last_chunk_)
						{
							src.ReleaseLastChunk<kThreadSafe>();
						}
						else
						{
							IF_TEST_STUFF(std::memset(&src.ElementsInLastChunk()[remaining_elements_in_last_chunk_src], 0xEEEE, sizeof(T) * (kElementsPerChunk - remaining_elements_in_last_chunk_src)));
						}
					}
				}
				else
				{
					for (int i = 0; i < num_elements_to_move; i++)
					{
						dst.push_back<kThreadSafe>(src.back());
						src.pop_back<kThreadSafe>();
					}
				}

				if (src.empty()) { return; }
				
				//Re-bind chunks
				//last chunk in dst or in src is full
				Assert(dst.number_of_elements_in_last_chunk_ == kElementsPerChunk || src.number_of_elements_in_last_chunk_ == kElementsPerChunk);
				if (dst.number_of_elements_in_last_chunk_ == kElementsPerChunk)
				{	//src goes last
					GetPtr(dst.last_chunk_)->next_chunk_ = GetPtr(src.first_chunk_);
					GetPtr(src.first_chunk_)->previous_chunk_ = GetPtr(dst.last_chunk_);
					dst.last_chunk_ = src.last_chunk_;

					dst.number_of_elements_in_last_chunk_ = src.number_of_elements_in_last_chunk_;
				}
				else
				{	//src goes first
					GetPtr(src.last_chunk_)->next_chunk_ = GetPtr(dst.first_chunk_);
					GetPtr(dst.first_chunk_)->previous_chunk_ = GetPtr(src.last_chunk_);
					dst.first_chunk_ = src.first_chunk_;
				}
				dst.number_chunks_ += src.number_chunks_;
			}
			src.first_chunk_ = kNullIndex;
			src.last_chunk_ = kNullIndex;
			src.number_chunks_ = 0;
			src.number_of_elements_in_last_chunk_ = kElementsPerChunk;

			IF_TEST_STUFF(src.ValidateNumberOfChunks());
			IF_TEST_STUFF(dst.ValidateNumberOfChunks());
		}

		template<class It, bool kThreadSafe>
		void Insert(It begin, It end)
		{
			for (; begin != end; ++begin)
			{
				push_back<kThreadSafe>(*begin);
			}
		}
	};
}